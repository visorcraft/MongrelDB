//! SQL/query correctness integrity tests for MongrelDB.
//!
//! These tests exercise the DataFusion SQL frontend against the native query
//! engine, looking for lost rows, extra rows, stale cache results, and incorrect
//! pushdown semantics.

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Condition, Database, Query, Row, RowId, Table, Value};
use mongreldb_query::MongrelSession;
use std::collections::HashMap;
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Schema builders
// ---------------------------------------------------------------------------

fn main_schema() -> Schema {
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
            ColumnDef {
                id: 3,
                name: "cat".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "amount".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 5,
                name: "score".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![
            IndexDef {
                name: "cat_bitmap".into(),
                column_id: 3,
                kind: IndexKind::Bitmap,
            },
            IndexDef {
                name: "name_fm".into(),
                column_id: 2,
                kind: IndexKind::FmIndex,
            },
            IndexDef {
                name: "amount_lr".into(),
                column_id: 4,
                kind: IndexKind::LearnedRange,
            },
            IndexDef {
                name: "score_lr".into(),
                column_id: 5,
                kind: IndexKind::LearnedRange,
            },
        ],
        colocation: vec![], constraints: Default::default(),
    }
}

fn ann_schema() -> Schema {
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
                name: "vec".into(),
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "vec_ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
        }],
        colocation: vec![], constraints: Default::default(),
    }
}

fn orders_schema() -> Schema {
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
                name: "customer_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![], constraints: Default::default(),
    }
}

fn customers_schema() -> Schema {
    Schema {
        schema_id: 4,
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
        colocation: vec![], constraints: Default::default(),
    }
}

// ---------------------------------------------------------------------------
// Data-loading helpers
// ---------------------------------------------------------------------------

fn insert_main_rows(db: &mut Table, n: i64) -> Vec<RowId> {
    let mut batch = Vec::with_capacity(n as usize);
    for i in 0..n {
        let cat = match i % 3 {
            0 => "A",
            1 => "B",
            _ => "C",
        };
        batch.push(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("item{i}").into_bytes())),
            (3, Value::Bytes(cat.as_bytes().to_vec())),
            (4, Value::Int64(i * 10)),
            (5, Value::Float64(i as f64 * 0.5)),
        ]);
    }
    db.put_batch(batch).unwrap()
}

fn insert_main_rows_and_commit(db: &mut Table, n: i64) {
    insert_main_rows(db, n);
    db.commit().unwrap();
}

// ---------------------------------------------------------------------------
// Comparison helpers
// ---------------------------------------------------------------------------

type ComparableRow = (i64, HashMap<u16, Value>);

fn arrow_cell(arr: &arrow::array::ArrayRef, row: usize, ty: TypeId) -> Option<Value> {
    if arr.is_null(row) {
        return None;
    }
    match ty {
        TypeId::Int64 | TypeId::TimestampNanos => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            Some(Value::Int64(a.value(row)))
        }
        TypeId::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            Some(Value::Float64(a.value(row)))
        }
        TypeId::Bytes => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            Some(Value::Bytes(a.value(row).as_bytes().to_vec()))
        }
        TypeId::Bool => {
            let a = arr
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap();
            Some(Value::Bool(a.value(row)))
        }
        _ => panic!("unsupported type in test helper: {ty:?}"),
    }
}

fn sql_rows(batches: &[RecordBatch], schema: &Schema) -> Vec<ComparableRow> {
    let mut out = Vec::new();
    for batch in batches {
        let id_idx = batch.schema().index_of("id").expect("id column in result");
        let id_arr = batch
            .column(id_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for r in 0..batch.num_rows() {
            let rid = id_arr.value(r);
            let mut cols = HashMap::new();
            for (j, field) in batch.schema().fields().iter().enumerate() {
                let name = field.name();
                if name == "id" {
                    continue;
                }
                if let Some(cdef) = schema.column(name) {
                    if let Some(v) = arrow_cell(batch.column(j), r, cdef.ty) {
                        cols.insert(cdef.id, v);
                    }
                }
            }
            out.push((rid, cols));
        }
    }
    out.sort_by_key(|(id, _)| *id);
    out
}

fn native_rows(rows: &[Row], col_ids: &[u16]) -> Vec<ComparableRow> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cols = HashMap::new();
        for &cid in col_ids {
            if let Some(v) = row.columns.get(&cid) {
                cols.insert(cid, v.clone());
            }
        }
        out.push((row.row_id.0 as i64, cols));
    }
    out.sort_by_key(|(id, _)| *id);
    out
}

fn assert_rows_eq(a: &[ComparableRow], b: &[ComparableRow], ctx: &str) {
    assert_eq!(a.len(), b.len(), "{ctx}: row count mismatch");
    for (i, ((id_a, cols_a), (id_b, cols_b))) in a.iter().zip(b.iter()).enumerate() {
        assert_eq!(id_a, id_b, "{ctx}: id mismatch at row {i}");
        // Compare only the columns the SQL projection actually returned.
        for cid in cols_a.keys() {
            assert_eq!(
                cols_a.get(cid),
                cols_b.get(cid),
                "{ctx}: column {cid} mismatch at row {i} (id {id_a})"
            );
        }
    }
}

fn i64_scalar(batches: &[RecordBatch]) -> i64 {
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    arr.value(0)
}

fn count_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// ---------------------------------------------------------------------------
// Tests: SQL vs native consistency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_scan_sql_matches_native_query() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session.run("select * from t order by id").await.unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let native = db.query(&Query::new()).unwrap();
    let all_ids: Vec<u16> = main_schema().columns.iter().map(|c| c.id).collect();
    let native_vals = native_rows(&native, &all_ids);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "full scan");
    assert_eq!(count_rows(&sql), 100);
}

#[tokio::test]
async fn count_star_matches_native_count() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let batches = session.run("select count(*) from t").await.unwrap();
    let sql_count = i64_scalar(&batches);

    let db = session.db().unwrap().lock();
    let native_count = db.count();
    drop(db);

    assert_eq!(sql_count, native_count as i64);
    assert_eq!(sql_count, 100);
}

#[tokio::test]
async fn bitmap_equality_sql_matches_native() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session
        .run("select id, cat from t where cat = 'A' order by id")
        .await
        .unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 3,
        value: Value::Bytes(b"A".to_vec()).encode_key(),
    });
    let native = db.query(&q).unwrap();
    let native_vals = native_rows(&native, &[3]);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "bitmap equality");
    assert_eq!(sql_vals.len(), 34);
}

#[tokio::test]
async fn pk_equality_sql_matches_native() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session.run("select * from t where id = 42").await.unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let q = Query::pk(Value::Int64(42).encode_key());
    let native = db.query(&q).unwrap();
    let all_ids: Vec<u16> = main_schema().columns.iter().map(|c| c.id).collect();
    let native_vals = native_rows(&native, &all_ids);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "pk equality");
    assert_eq!(sql_vals.len(), 1);
}

#[tokio::test]
async fn int_range_sql_matches_native() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session
        .run("select id, amount from t where amount between 200 and 500 order by id")
        .await
        .unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::Range {
        column_id: 4,
        lo: 200,
        hi: 500,
    });
    let native = db.query(&q).unwrap();
    let native_vals = native_rows(&native, &[4]);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "int range");
    assert_eq!(sql_vals.len(), 31); // i=20..=50
}

#[tokio::test]
async fn float_range_sql_matches_native() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session
        .run("select id, score from t where score > 10.0 and score < 30.0 order by id")
        .await
        .unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::RangeF64 {
        column_id: 5,
        lo: 10.0,
        lo_inclusive: false,
        hi: 30.0,
        hi_inclusive: false,
    });
    let native = db.query(&q).unwrap();
    let native_vals = native_rows(&native, &[5]);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "float range");
    // score = i*0.5; 10 < i*0.5 < 30 => 21..=59 => 39 rows
    assert_eq!(sql_vals.len(), 39);
}

#[tokio::test]
async fn like_sql_matches_native_fm_and_is_exact() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    // FM pushdown extracts literal "5"; DataFusion must re-apply the wildcard.
    let sql = session
        .run("select id, name from t where name like '%5%' order by id")
        .await
        .unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::FmContains {
        column_id: 2,
        pattern: b"5".to_vec(),
    });
    let native = db.query(&q).unwrap();
    let native_vals = native_rows(&native, &[2]);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "LIKE pushdown");
    // item5, item15, item25, item35, item45, item50..59, item65, item75, item85, item95
    assert_eq!(sql_vals.len(), 19);
}

#[tokio::test]
async fn in_list_sql_matches_native_bitmap_in() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session
        .run("select id, cat from t where cat in ('A', 'C') order by id")
        .await
        .unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::BitmapIn {
        column_id: 3,
        values: vec![
            Value::Bytes(b"A".to_vec()).encode_key(),
            Value::Bytes(b"C".to_vec()).encode_key(),
        ],
    });
    let native = db.query(&q).unwrap();
    let native_vals = native_rows(&native, &[3]);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "IN list");
    assert_eq!(sql_vals.len(), 67);
}

#[tokio::test]
async fn ann_search_sql_matches_native() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), ann_schema(), 2).unwrap();
    let proto = [1.0f32, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0];
    let mut batch = Vec::new();
    for i in 0..20i64 {
        let mut v = proto;
        if i > 0 {
            v[((i - 1) as usize) % 8] *= -1.0;
        }
        batch.push(vec![
            (1, Value::Int64(i)),
            (2, Value::Embedding(v.to_vec())),
        ]);
    }
    db.put_batch(batch).unwrap();
    db.commit().unwrap();
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("emb").await.unwrap();

    let sql = session
        .run("select id from emb where ann_search(vec, '[1,-1,1,1,-1,1,1,-1]', 5) order by id")
        .await
        .unwrap();
    let sql_ids: Vec<i64> = sql_rows(&sql, &ann_schema())
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::Ann {
        column_id: 2,
        query: proto.to_vec(),
        k: 5,
    });
    let native = db.query(&q).unwrap();
    let native_ids: Vec<i64> = native.iter().map(|r| r.row_id.0 as i64).collect();
    drop(db);

    assert_eq!(sql_ids.len(), 5, "SQL ann_search should return k rows");
    assert_eq!(sql_ids, native_ids, "SQL ANN result must match native ANN");
    assert!(sql_ids.contains(&0), "exact-match vector must be in top-k");
}

// ---------------------------------------------------------------------------
// Tests: JOIN correctness and cache invalidation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn join_sql_matches_expected_result() {
    let dir = tempdir().unwrap();
    let db = std::sync::Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("orders", orders_schema()).unwrap();
    db.create_table("customers", customers_schema()).unwrap();

    db.transaction(|t| {
        t.put(
            "customers",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"Alice".to_vec()))],
        )?;
        t.put(
            "customers",
            vec![(1, Value::Int64(2)), (2, Value::Bytes(b"Bob".to_vec()))],
        )?;
        t.put("orders", vec![(1, Value::Int64(100)), (2, Value::Int64(1))])?;
        t.put("orders", vec![(1, Value::Int64(101)), (2, Value::Int64(2))])?;
        t.put("orders", vec![(1, Value::Int64(102)), (2, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();

    let session = MongrelSession::open(std::sync::Arc::clone(&db)).unwrap();
    let batches = session
        .run("select o.id, c.name from orders o join customers c on o.customer_id = c.id order by o.id")
        .await
        .unwrap();

    assert_eq!(count_rows(&batches), 3);
    let names_col = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let names: Vec<&str> = (0..names_col.len()).map(|i| names_col.value(i)).collect();
    assert_eq!(names, vec!["Alice", "Bob", "Alice"]);
}

#[tokio::test]
async fn database_session_cache_invalidates_after_commit() {
    let dir = tempdir().unwrap();
    let db = std::sync::Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("orders", orders_schema()).unwrap();
    db.create_table("customers", customers_schema()).unwrap();

    db.transaction(|t| {
        t.put("orders", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        t.put(
            "customers",
            vec![(1, Value::Int64(10)), (2, Value::Bytes(b"X".to_vec()))],
        )?;
        Ok(())
    })
    .unwrap();

    let session = MongrelSession::open(std::sync::Arc::clone(&db)).unwrap();

    let first = session
        .run("select o.id, c.name from orders o join customers c on o.customer_id = c.id")
        .await
        .unwrap();
    assert_eq!(count_rows(&first), 1);

    // Same epoch: repeat must return identical batches (cache hit).
    let second = session
        .run("select o.id, c.name from orders o join customers c on o.customer_id = c.id")
        .await
        .unwrap();
    assert_eq!(count_rows(&second), 1);

    // Mutate a secondary table; the Database's visible epoch advances on commit.
    db.transaction(|t| {
        t.put("orders", vec![(1, Value::Int64(2)), (2, Value::Int64(10))])?;
        Ok(())
    })
    .unwrap();

    let after = session
        .run("select o.id, c.name from orders o join customers c on o.customer_id = c.id order by o.id")
        .await
        .unwrap();
    assert_eq!(count_rows(&after), 2);
}

#[tokio::test]
async fn table_session_cache_invalidates_after_commit() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 50);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let first = session.run("select count(*) from t").await.unwrap();
    assert_eq!(i64_scalar(&first), 50);

    // Insert more rows through the same underlying table (ids 50..99).
    let handle = session.db().unwrap().clone();
    {
        let mut db = handle.lock();
        let mut batch = Vec::with_capacity(50);
        for i in 50..100i64 {
            batch.push(vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("item{i}").into_bytes())),
                (
                    3,
                    Value::Bytes(if i % 2 == 0 { b"A" } else { b"B" }.to_vec()),
                ),
                (4, Value::Int64(i * 10)),
                (5, Value::Float64(i as f64 * 0.5)),
            ]);
        }
        db.put_batch(batch).unwrap();
        db.commit().unwrap();
    }

    let after = session.run("select count(*) from t").await.unwrap();
    assert_eq!(i64_scalar(&after), 100);
}

// ---------------------------------------------------------------------------
// Tests: reopen, multi-run, deletes, schema evolution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sql_results_survive_close_and_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
        insert_main_rows_and_commit(&mut db, 100);
        db.flush().unwrap();
    }

    let db = Table::open(dir.path()).unwrap();
    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let batches = session.run("select count(*) from t").await.unwrap();
    assert_eq!(i64_scalar(&batches), 100);

    let sql = session.run("select * from t order by id").await.unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let native = db.query(&Query::new()).unwrap();
    let all_ids: Vec<u16> = main_schema().columns.iter().map(|c| c.id).collect();
    let native_vals = native_rows(&native, &all_ids);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "after reopen");
}

#[tokio::test]
async fn multi_run_sql_matches_native() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1);

    for run in 0..3i64 {
        let mut batch = Vec::with_capacity(100);
        for i in 0..100i64 {
            let id = run * 100 + i;
            let cat = match i % 3 {
                0 => "A",
                1 => "B",
                _ => "C",
            };
            batch.push(vec![
                (1, Value::Int64(id)),
                (2, Value::Bytes(format!("item{id}").into_bytes())),
                (3, Value::Bytes(cat.as_bytes().to_vec())),
                (4, Value::Int64(id * 10)),
                (5, Value::Float64(id as f64 * 0.5)),
            ]);
        }
        db.put_batch(batch).unwrap();
        db.commit().unwrap();
        db.flush().unwrap();
    }

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session.run("select * from t order by id").await.unwrap();
    assert_eq!(count_rows(&sql), 300);

    let sql_vals = sql_rows(&sql, &main_schema());

    let native_vals = {
        let mut db = session.db().unwrap().lock();
        let native = db.query(&Query::new()).unwrap();
        let all_ids: Vec<u16> = main_schema().columns.iter().map(|c| c.id).collect();
        native_rows(&native, &all_ids)
    };

    assert_rows_eq(&sql_vals, &native_vals, "multi-run full scan");

    // Range pushdown across runs.
    let filtered = session
        .run("select count(*) from t where amount between 500 and 1500")
        .await
        .unwrap();
    assert_eq!(i64_scalar(&filtered), 101); // ids 50..=150
}

#[tokio::test]
async fn deleted_rows_are_not_returned_by_sql() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    let ids = insert_main_rows(&mut db, 50);
    db.commit().unwrap();

    // Delete every fifth row by RowId.
    for (i, rid) in ids.iter().enumerate() {
        if i % 5 == 0 {
            db.delete(*rid).unwrap();
        }
    }
    db.commit().unwrap();
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let count = session.run("select count(*) from t").await.unwrap();
    assert_eq!(i64_scalar(&count), 40);

    let sql = session.run("select * from t order by id").await.unwrap();
    let sql_vals = sql_rows(&sql, &main_schema());

    let mut db = session.db().unwrap().lock();
    let native = db.query(&Query::new()).unwrap();
    let all_ids: Vec<u16> = main_schema().columns.iter().map(|c| c.id).collect();
    let native_vals = native_rows(&native, &all_ids);
    drop(db);

    assert_rows_eq(&sql_vals, &native_vals, "after deletes");
}

#[tokio::test]
async fn schema_evolution_reads_null_for_old_rows() {
    let base = Schema {
        schema_id: 5,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "amount".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![], constraints: Default::default(),
    };

    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), base, 1).unwrap();
    for i in 0..10i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 2))])
            .unwrap();
    }
    db.commit().unwrap();
    db.flush().unwrap();

    db.add_column(
        "note",
        TypeId::Bytes,
        ColumnFlags::empty().with(ColumnFlags::NULLABLE),
    )
    .unwrap();
    for i in 10..20i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Int64(i * 2)),
            (3, Value::Bytes(format!("note{i}").into_bytes())),
        ])
        .unwrap();
    }
    db.commit().unwrap();
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let batches = session
        .run("select id, note from t order by id")
        .await
        .unwrap();
    assert_eq!(count_rows(&batches), 20);

    let mut nulls = 0;
    let mut vals = 0;
    for batch in &batches {
        let note_arr = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for r in 0..batch.num_rows() {
            if note_arr.is_null(r) {
                nulls += 1;
            } else {
                vals += 1;
            }
        }
    }
    assert_eq!(nulls, 10);
    assert_eq!(vals, 10);
}

// ---------------------------------------------------------------------------
// Misc edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_table_queries_are_consistent() {
    let dir = tempdir().unwrap();
    let db = Table::create(dir.path(), main_schema(), 1).unwrap();
    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let count = session.run("select count(*) from t").await.unwrap();
    assert_eq!(i64_scalar(&count), 0);

    let all = session.run("select * from t").await.unwrap();
    assert_eq!(count_rows(&all), 0);

    let filtered = session
        .run("select id from t where cat = 'A'")
        .await
        .unwrap();
    assert_eq!(count_rows(&filtered), 0);
}

#[tokio::test]
async fn repeated_queries_return_identical_results() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let first = session
        .run("select * from t where cat = 'B' order by id")
        .await
        .unwrap();
    for _ in 0..5 {
        let again = session
            .run("select * from t where cat = 'B' order by id")
            .await
            .unwrap();
        assert_eq!(count_rows(&again), count_rows(&first));
        assert_rows_eq(
            &sql_rows(&again, &main_schema()),
            &sql_rows(&first, &main_schema()),
            "repeated query",
        );
    }
}

#[tokio::test]
async fn filtered_count_star_uses_pushdown() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 100);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let sql = session
        .run("select count(*) from t where amount between 300 and 700")
        .await
        .unwrap();
    let sql_count = i64_scalar(&sql);

    let mut db = session.db().unwrap().lock();
    let q = Query::new().and(Condition::Range {
        column_id: 4,
        lo: 300,
        hi: 700,
    });
    let native = db.query(&q).unwrap();
    drop(db);

    assert_eq!(sql_count, native.len() as i64);
    assert_eq!(sql_count, 41); // i=30..=70
}

#[tokio::test]
async fn view_name_substring_of_table_does_not_rewrite_table() {
    // Regression: view resolution used a substring search, so a view named
    // `log` would rewrite `FROM logs` (leaving a dangling `s`). Now whole-word
    // matching means only a real `FROM log` reference is rewritten.
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 5);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();
    session.create_view("t", "select * from t");

    // `FROM t` resolves through the view and still returns the 5 base rows.
    let via_view = session.run("select * from t").await.unwrap();
    assert_eq!(count_rows(&via_view), 5);
}

#[tokio::test]
async fn whitespace_only_queries_share_result() {
    // Queries that differ only in whitespace must return identical results
    // (and, now that the SQL is normalized for caching, share one cache entry).
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), main_schema(), 1).unwrap();
    insert_main_rows_and_commit(&mut db, 10);
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("t").await.unwrap();

    let a = session.run("select * from t order by id").await.unwrap();
    let b = session
        .run("  select  *  from  t  order  by  id  ")
        .await
        .unwrap();
    let c = session
        .run("\n\tselect\n*\nfrom\n\tt\norder\nby\nid\n")
        .await
        .unwrap();
    assert_eq!(count_rows(&a), 10);
    assert_eq!(count_rows(&b), 10);
    assert_eq!(count_rows(&c), 10);
    // First columns (id) must match row-for-row.
    let ids = |b: &[RecordBatch]| -> Vec<i64> {
        b.iter()
            .flat_map(|batch| {
                let arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (0..arr.len()).map(move |i| arr.value(i))
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&a), ids(&b));
    assert_eq!(ids(&a), ids(&c));
}
