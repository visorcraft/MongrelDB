//! SQL frontend tests: `select *`, `count(*)`, filters, projections, limits.

use arrow::array::Array;
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Table, Value};
use mongreldb_query::MongrelSession;
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
                name: "destination".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "departure".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "cost".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 5,
                name: "rating".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![
            mongreldb_core::schema::IndexDef {
                name: "dest_bitmap".into(),
                column_id: 2,
                kind: mongreldb_core::schema::IndexKind::Bitmap,
            },
            mongreldb_core::schema::IndexDef {
                name: "dest_fm".into(),
                column_id: 2,
                kind: mongreldb_core::schema::IndexKind::FmIndex,
            },
        ],
        colocation: vec![], constraints: Default::default(),
    }
}

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn setup() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    for i in 0..100i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("City{i}").into_bytes())),
            (3, Value::Int64(1_700_000_000 + i * 86_400)),
            (4, Value::Float64(199.99 + i as f64)),
            (5, Value::Float64(3.5 + (i % 3) as f64)),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    let session = MongrelSession::new(db);
    session.register("travel_trips").await.unwrap();
    (dir, session)
}

#[tokio::test]
async fn select_star_returns_all_rows() {
    let (_dir, session) = setup().await;
    let batches = session.run("select * from travel_trips").await.unwrap();
    assert_eq!(total_rows(&batches), 100);
    // All five user columns projected.
    let expected_cols = 5;
    assert_eq!(
        batches[0].schema().fields().len(),
        expected_cols,
        "select * should expose the user columns"
    );
}

#[tokio::test]
async fn count_star_is_correct() {
    let (_dir, session) = setup().await;
    let batches = session
        .run("select count(*) as n from travel_trips")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 1);
    // The single count cell equals 100.
    let col = batches[0].column(0);
    let val = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(val.value(0), 100);
}

#[tokio::test]
async fn where_filter_prunes_rows() {
    let (_dir, session) = setup().await;
    // cost = 199.99 + i  ⇒ cost < 250 ⇒ i ≤ 50 ⇒ 51 rows.
    let batches = session
        .run("select * from travel_trips where cost < 250")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 51);
}

#[tokio::test]
async fn projection_and_limit() {
    let (_dir, session) = setup().await;
    let batches = session
        .run("select destination, cost from travel_trips order by cost desc limit 5")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 5);
    assert_eq!(batches[0].schema().fields().len(), 2);
}

#[tokio::test]
async fn predicate_pushdown_filtered_query() {
    let (_dir, session) = setup().await;
    // destination has a bitmap index; this WHERE should push through the index
    // instead of scanning the full table.
    let batches = session
        .run("select * from travel_trips where destination = 'City50'")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 1, "filtered to exactly one row");

    // PK equality pushdown.
    let batches2 = session
        .run("select id from travel_trips where id = 42")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches2), 1);
}

#[tokio::test]
async fn result_cache_returns_same_batches_on_repeat() {
    let (_dir, session) = setup().await;
    let b1 = session
        .run("select count(*) as n from travel_trips")
        .await
        .unwrap();
    let b2 = session
        .run("select count(*) as n from travel_trips")
        .await
        .unwrap();
    assert_eq!(total_rows(&b1), total_rows(&b2));
    // After clearing, a fresh execution still produces the same answer.
    session.clear_cache();
    let b3 = session
        .run("select count(*) as n from travel_trips")
        .await
        .unwrap();
    assert_eq!(total_rows(&b1), total_rows(&b3));
}

#[tokio::test]
async fn aggregation_groups_by() {
    let (_dir, session) = setup().await;
    // rating ∈ {3.5, 4.5, 5.5} ⇒ 3 groups.
    let batches = session
        .run("select rating, count(*) as n from travel_trips group by rating order by rating")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 3);
}

// ── Item 1: range / LIKE / float-range predicate pushdown ───────────────────

/// Int64 range pushdown: `departure = 1.7e9 + i*86400`, so a bounded range maps
/// to a known slice of `i`. Two conjuncted `>=`/`<=` filters each translate to a
/// `Condition::Range` and are intersected.
#[tokio::test]
async fn range_int64_pushdown() {
    let (_dir, session) = setup().await;
    let lo = 1_700_000_000i64;
    let hi = 1_700_000_000 + 10 * 86_400;
    let sql = format!(
        "select id from travel_trips where departure >= {lo} and departure <= {hi} order by id"
    );
    let batches = session.run(&sql).await.unwrap();
    // i ∈ 0..=10 ⇒ 11 rows.
    assert_eq!(total_rows(&batches), 11);
    // First id is 0, last is 10.
    let col = batches[0].column(0);
    let vals = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(vals.value(0), 0);
    assert_eq!(vals.value(vals.len() - 1), 10);
}

/// `BETWEEN` on an Int64 column → `Condition::Range`.
#[tokio::test]
async fn range_between_pushdown() {
    let (_dir, session) = setup().await;
    let lo = 1_700_000_000i64 + 5 * 86_400;
    let hi = 1_700_000_000 + 8 * 86_400;
    let sql = format!("select id from travel_trips where departure between {lo} and {hi}");
    let batches = session.run(&sql).await.unwrap();
    // i ∈ 5..=8 ⇒ 4 rows.
    assert_eq!(total_rows(&batches), 4);
}

/// Float64 range pushdown: `cost = 199.99 + i`, `cost < 250` ⇒ i ≤ 50 ⇒ 51.
/// (This now exercises `Condition::RangeF64` instead of a full scan.)
#[tokio::test]
async fn range_float64_pushdown() {
    let (_dir, session) = setup().await;
    let batches = session
        .run("select id from travel_trips where cost < 250 order by id")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 51);
    let col = batches[0].column(0);
    let vals = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(vals.value(vals.len() - 1), 50);
}

/// `LIKE 'City5%'` → FM-index substring "City5" (City5, City50–City59 ⇒ 11).
#[tokio::test]
async fn like_fm_pushdown() {
    let (_dir, session) = setup().await;
    let batches = session
        .run("select id from travel_trips where destination like 'City5%' order by id")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 11);
}

/// Cross-condition intersection: bitmap equality ∩ int range. `destination =
/// 'City5'` (1 row) AND `departure` in range → still just that one row.
#[tokio::test]
async fn bitmap_intersect_range() {
    let (_dir, session) = setup().await;
    let sql = "select id from travel_trips \
               where destination = 'City5' and departure >= 0 and departure <= 1701000000";
    let batches = session.run(sql).await.unwrap();
    assert_eq!(total_rows(&batches), 1);
}

// ── Priority 6: OR-of-equalities on one column → BitmapIn pushdown ────────────

/// `col = a OR col = b OR …` on a bitmap-indexed column unions to the bitmap of
/// each value. Exercises `try_or_as_bitmap_in` and the `BitmapIn` condition.
#[tokio::test]
async fn or_of_equalities_same_column_unions() {
    let (_dir, session) = setup().await;
    let two = session
        .run("select id from travel_trips where destination = 'City5' or destination = 'City7'")
        .await
        .unwrap();
    assert_eq!(total_rows(&two), 2);

    let three = session
        .run(
            "select id from travel_trips \
             where destination = 'City1' or destination = 'City2' or destination = 'City3'",
        )
        .await
        .unwrap();
    assert_eq!(total_rows(&three), 3);
}

// ── Priority 6: IS NULL / IS NOT NULL pushdown ───────────────────────────────

/// 60 rows predate an `add_column("note")` (so they read NULL); 40 rows set it.
/// `IS NULL` must return exactly the 60, `IS NOT NULL` the 40.
#[tokio::test]
async fn is_null_and_is_not_null_partition_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    for i in 0..60i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("City{i}").into_bytes())),
            (3, Value::Int64(1_700_000_000 + i)),
            (4, Value::Float64(1.0 + i as f64)),
            (5, Value::Float64(2.0)),
        ])
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
    for i in 60..100i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("City{i}").into_bytes())),
            (3, Value::Int64(1_700_000_000 + i)),
            (4, Value::Float64(1.0 + i as f64)),
            (5, Value::Float64(2.0)),
            (6, Value::Bytes(format!("note{i}").into_bytes())),
        ])
        .unwrap();
    }
    db.commit().unwrap();
    db.flush().unwrap();

    let session = MongrelSession::new(db);
    session.register("travel_trips").await.unwrap();

    let nulls = session
        .run("select id from travel_trips where note is null")
        .await
        .unwrap();
    assert_eq!(total_rows(&nulls), 60);

    let not_nulls = session
        .run("select id from travel_trips where note is not null")
        .await
        .unwrap();
    assert_eq!(total_rows(&not_nulls), 40);
}

// ── Item 1: HNSW semantic pushdown via the `ann_search` SQL UDF ──────────────

fn vec_schema() -> Schema {
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
        indexes: vec![mongreldb_core::schema::IndexDef {
            name: "vec_ann".into(),
            column_id: 2,
            kind: mongreldb_core::schema::IndexKind::Ann,
        }],
        colocation: vec![], constraints: Default::default(),
    }
}

/// `ann_search(vec, '[...]', k)` → HNSW top-k. The query vector equals row 0's
/// embedding, so row 0 must appear in the top-3.
#[tokio::test]
async fn ann_search_pushdown() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), vec_schema(), 2).unwrap();
    let proto = [1.0f32, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0];
    for i in 0..12i64 {
        // Row 0 is the prototype itself; every other row flips a distinct bit.
        let mut v = proto;
        if i > 0 {
            v[((i - 1) as usize) % 8] *= -1.0;
        }
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Embedding(v.to_vec())),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    let session = MongrelSession::new(db);
    session.register("items").await.unwrap();

    let sql = "select id from items where ann_search(vec, '[1,-1,1,1,-1,1,1,-1]', 3) order by id";
    let batches = session.run(sql).await.unwrap();
    // Exactly k = 3 rows.
    assert_eq!(total_rows(&batches), 3);
    let col = batches[0].column(0);
    let vals = col
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let ids: Vec<i64> = vals.values().iter().copied().collect();
    // Row 0 is identical to the query (Hamming distance 0) ⇒ must be present.
    assert!(
        ids.contains(&0),
        "top-k must include the exact match: {ids:?}"
    );
}

// ── Item 3: multi-table joins across separately-registered tables ────────────

fn cities_schema() -> Schema {
    Schema {
        schema_id: 3,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "city_name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "country".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![mongreldb_core::schema::IndexDef {
            name: "country_bitmap".into(),
            column_id: 2,
            kind: mongreldb_core::schema::IndexKind::Bitmap,
        }],
        colocation: vec![], constraints: Default::default(),
    }
}

/// Two distinct tables (separate `Table`s) on one session → DataFusion hash join.
/// `travel_trips.destination ∈ City0..City99`; `cities` lists City0..City9 with
/// `country ∈ {North, South}`. A filtered join on `country = 'North'` yields the
/// 5 matching trips.
#[tokio::test]
async fn multi_table_join() {
    let (dir, session) = setup().await; // registers travel_trips

    let cities_dir = tempdir().unwrap();
    let mut cities = Table::create(cities_dir.path(), cities_schema(), 3).unwrap();
    for i in 0..10i64 {
        let country = if i % 2 == 0 { "North" } else { "South" };
        cities
            .put(vec![
                (1, Value::Bytes(format!("City{i}").into_bytes())),
                (2, Value::Bytes(country.as_bytes().to_vec())),
            ])
            .unwrap();
    }
    cities.flush().unwrap();
    session.register_db("cities", cities).await.unwrap();

    let sql = "select t.destination, c.country \
               from travel_trips t join cities c on t.destination = c.city_name \
               where c.country = 'North' \
               order by t.destination";
    let batches = session.run(sql).await.unwrap();
    // City0,2,4,6,8 ⇒ 5 rows.
    assert_eq!(total_rows(&batches), 5);
    let _ = dir;
}

/// Phase 8.1: the FK-join intercept (bitmap intersection) serves the join with
/// a predicate on **both** sides — a PK-side bitmap filter (`country = 'North'`)
/// intersected with an FK-side range (`departure >= …`). departure =
/// 1_700_000_000 + i·86_400; the threshold selects i ≥ 5. North cities are
/// City0,2,4,6,8; only City6 (i=6) and City8 (i=8) survive both filters ⇒ 2.
#[tokio::test]
async fn fk_join_composes_pk_and_fk_predicates() {
    let (dir, session) = setup().await;

    let cities_dir = tempdir().unwrap();
    let mut cities = Table::create(cities_dir.path(), cities_schema(), 3).unwrap();
    for i in 0..10i64 {
        let country = if i % 2 == 0 { "North" } else { "South" };
        cities
            .put(vec![
                (1, Value::Bytes(format!("City{i}").into_bytes())),
                (2, Value::Bytes(country.as_bytes().to_vec())),
            ])
            .unwrap();
    }
    cities.flush().unwrap();
    session.register_db("cities", cities).await.unwrap();

    let sql = "select t.destination, c.country \
               from travel_trips t join cities c on t.destination = c.city_name \
               where c.country = 'North' and t.departure >= 1700432000 \
               order by t.destination";
    let batches = session.run(sql).await.unwrap();
    assert_eq!(
        total_rows(&batches),
        2,
        "only City6 and City8 pass both filters"
    );
    // Ordered ascending ⇒ first destination is City6.
    let arr = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(arr.value(0), "City6");
    let _ = dir;
}

/// Phase 6.1: the scan streams one `RecordBatch` per 65 536-row chunk instead
/// of one giant batch. A table spanning multiple pages must come back as several
/// capped batches whose sizes sum to the row count, and `LIMIT`/`COUNT(*)` must
/// behave correctly across the batch boundary.
#[tokio::test]
async fn streaming_scan_emits_multiple_batches() {
    let dir = tempdir().unwrap();
    let minimal = Schema {
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![], constraints: Default::default(),
    };
    let mut db = Table::create(dir.path(), minimal, 1).unwrap();
    // 65 536 + 1000 ⇒ exactly two streamed batches (full + partial).
    let n: i64 = 65_536 + 1000;
    for i in 0..n {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 2))])
            .unwrap();
    }
    db.flush().unwrap();
    let session = MongrelSession::new(db);
    session.register("nums").await.unwrap();

    // select * → all rows, chunked into ≤65 536-row batches.
    let batches = session.run("select * from nums").await.unwrap();
    assert_eq!(total_rows(&batches), n as usize);
    assert!(
        batches.len() >= 2,
        "expected multiple streamed batches, got {}",
        batches.len()
    );
    assert!(batches.iter().all(|b| b.num_rows() <= 65_536));
    // Every batch but the last must be exactly full.
    for b in &batches[..batches.len() - 1] {
        assert_eq!(b.num_rows(), 65_536);
    }

    // COUNT(*) is correct across the multi-batch data.
    let c = session.run("select count(*) as n from nums").await.unwrap();
    let col = c[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), n);

    // LIMIT short-circuits within the first chunk.
    let lim = session.run("select * from nums limit 10").await.unwrap();
    assert_eq!(total_rows(&lim), 10);

    // A range filter must still be exact across batch boundaries.
    let half = session
        .run("select * from nums where v < 1000")
        .await
        .unwrap();
    // v = i*2 < 1000 ⇒ i < 500 ⇒ 500 rows.
    assert_eq!(total_rows(&half), 500);
    let _ = dir;
}

/// Phase 16.1/16.2: a multi-run table streams through the k-way-merge cursor
/// (not the materialize-then-chunk fallback), so `LIMIT` short-circuits and a
/// bitmap equality predicate is exact across runs.
#[tokio::test]
async fn multi_run_streams_and_limit_short_circuits() {
    use mongreldb_core::schema::{IndexDef, IndexKind};
    let dir = tempdir().unwrap();
    let multi_schema = Schema {
        schema_id: 9,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "v_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
        }],
        colocation: vec![], constraints: Default::default(),
    };
    let mut db = Table::create(dir.path(), multi_schema, 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // each flush spills a fresh run
    for run in 0..3i64 {
        for i in 0..1000i64 {
            let id = run * 10_000 + i;
            db.put(vec![(1, Value::Int64(id)), (2, Value::Int64(id))])
                .unwrap();
        }
        db.flush().unwrap();
    }
    assert!(db.run_count() >= 3, "multi-run layout");

    let session = MongrelSession::new(db);
    session.register("nums").await.unwrap();

    // select * streams all 3000 rows via the multi-run cursor.
    let all = session.run("select * from nums").await.unwrap();
    assert_eq!(total_rows(&all), 3000);

    // LIMIT short-circuits without draining every run.
    let lim = session.run("select * from nums limit 10").await.unwrap();
    assert_eq!(total_rows(&lim), 10);

    // Bitmap predicate is exact across runs: exactly one row has v = 10500.
    let one = session
        .run("select * from nums where v = 10500")
        .await
        .unwrap();
    assert_eq!(total_rows(&one), 1);
    let _ = dir;
}

/// Phase 6.2: the cursor fast path skips pages whose survivors don't match the
/// predicate and decodes only surviving pages' projected columns. Verify
/// correctness when matches land in a later page and when nothing matches.
#[tokio::test]
async fn cursor_page_pruning_is_exact() {
    let dir = tempdir().unwrap();
    let minimal = Schema {
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![], constraints: Default::default(),
    };
    let mut db = Table::create(dir.path(), minimal, 1).unwrap();
    // ~3 pages (2 full 65 536-row pages + a partial third).
    let n: i64 = 65_536 * 2 + 5000;
    for i in 0..n {
        // v is monotonically increasing, so ranges map to contiguous page spans.
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i))])
            .unwrap();
    }
    db.flush().unwrap();
    let session = MongrelSession::new(db);
    session.register("nums").await.unwrap();

    // Matches only in the third (partial) page — first two pages are skipped.
    let third = session
        .run("select * from nums where v >= 131000 and v < 131500")
        .await
        .unwrap();
    assert_eq!(total_rows(&third), 500);

    // Matches spanning the first/second page boundary.
    let span = session
        .run("select * from nums where v >= 65500 and v < 65600")
        .await
        .unwrap();
    assert_eq!(total_rows(&span), 100);

    // No matches at all (every page pruned by stats or filtered out).
    let none = session
        .run("select * from nums where v > 9999999")
        .await
        .unwrap();
    assert_eq!(total_rows(&none), 0);

    // COUNT(*) over the whole multi-page table.
    let c = session.run("select count(*) as n from nums").await.unwrap();
    let col = c[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), n);
    let _ = dir;
}

/// Phase 6.3: zero-copy Arrow conversion (`native_to_array_owned`) must round-
/// trip nullable Int64 columns correctly — nulls land as Arrow nulls, not as
/// garbage values, and `is_null` agrees with the source on every position.
#[tokio::test]
async fn zero_copy_preserves_nulls() {
    let dir = tempdir().unwrap();
    let nullable = Schema {
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![],
        colocation: vec![], constraints: Default::default(),
    };
    let mut db = Table::create(dir.path(), nullable, 1).unwrap();
    for i in 0..10i64 {
        // Even rows carry a value; odd rows leave `v` null (no entry).
        if i % 2 == 0 {
            db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 10))])
                .unwrap();
        } else {
            db.put(vec![(1, Value::Int64(i))]).unwrap();
        }
    }
    db.flush().unwrap();
    let session = MongrelSession::new(db);
    session.register("nums").await.unwrap();

    let batches = session.run("select v from nums").await.unwrap();
    assert_eq!(total_rows(&batches), 10);
    // Position i must be null iff i is odd.
    let mut pos = 0;
    for b in &batches {
        let arr = b
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for j in 0..arr.len() {
            let expected_null = pos % 2 == 1;
            assert_eq!(
                arr.is_null(j),
                expected_null,
                "pos {pos} null mismatch (expected {expected_null})"
            );
            if !expected_null {
                assert_eq!(arr.value(j), pos * 10);
            }
            pos += 1;
        }
    }
    let _ = dir;
}

/// Phase 6.2 regression: a column added via `add_column` after the run was
/// written must read as NULL through the cursor scan path (the cursor must not
/// fail with ColumnNotFound for an absent column).
#[tokio::test]
async fn cursor_handles_schema_evolution() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    for i in 0..100i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("City{i}").into_bytes())),
            (3, Value::Int64(1_700_000_000 + i * 86_400)),
            (4, Value::Float64(199.99 + i as f64)),
            (5, Value::Float64(3.5 + (i % 3) as f64)),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    // Add a nullable column AFTER the run was written — the old run has no
    // pages for it, so every existing row must read NULL.
    db.add_column(
        "note",
        TypeId::Bytes,
        ColumnFlags::empty().with(ColumnFlags::NULLABLE),
    )
    .unwrap();

    // Acquire the db back out of a freshly-built session for writes/queries.
    let session = MongrelSession::new(db);
    session.register("travel_trips").await.unwrap();

    // The new column exists in the table schema and is all-NULL for old rows.
    let batches = session.run("select note from travel_trips").await.unwrap();
    assert_eq!(total_rows(&batches), 100);
    for b in &batches {
        let arr = b.column(0);
        assert_eq!(arr.null_count(), arr.len());
    }

    // SELECT * still works (mix of present and absent columns) and returns 100
    // rows × 6 columns.
    let all = session.run("select * from travel_trips").await.unwrap();
    assert_eq!(total_rows(&all), 100);
    assert_eq!(all[0].schema().fields().len(), 6);
    let _ = dir;
}

/// Phase 7.1: `COUNT(*)` is served O(1) from the `live_count` metadata, and
/// `MIN`/`MAX` of an Int64 column from exact page stats (insert-only table),
/// via DataFusion's `AggregateStatistics` rewrite. All three must be exact.
#[tokio::test]
async fn metadata_aggregates_count_min_max() {
    let (_dir, session) = setup().await; // travel_trips: id 0..99, single flushed run.
    let c = session
        .run("select count(*) as n from travel_trips")
        .await
        .unwrap();
    let n = c[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 100);

    let mm = session
        .run("select min(id) as mn, max(id) as mx from travel_trips")
        .await
        .unwrap();
    let mn = mm[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    let mx = mm[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(mn, 0);
    assert_eq!(mx, 99);
}

/// Phase 7.1 (P7a/P7b): COUNT(col) and MIN/MAX served from page
/// min/max/null_count with no column decode. `bulk_load` lands the rows in a
/// single sorted run (empty overlay, `live_count == row_count`), so aggregates
/// route through `aggregate_from_stats`. `empty_i` is added afterward, so every
/// row reads NULL for it — exercising COUNT-excludes-NULL and all-NULL MIN/MAX
/// (⇒ SQL NULL).
#[tokio::test]
async fn metadata_aggregates_count_col_and_null_column() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    let rows: Vec<Vec<(u16, Value)>> = (0..100i64)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("City{i}").into_bytes())),
                (3, Value::Int64(1_700_000_000 + i)),
                (4, Value::Float64(10.0 + i as f64)),
                (5, Value::Float64(2.0)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap(); // single sorted run, 100 rows
    db.add_column(
        "empty_i",
        TypeId::Int64,
        ColumnFlags::empty().with(ColumnFlags::NULLABLE),
    )
    .unwrap(); // absent from the run ⇒ every row reads NULL, still one run
    let session = MongrelSession::new(db);
    session.register("travel_trips").await.unwrap();

    // COUNT(col) excludes NULLs: id (PK) has none; empty_i is wholly NULL.
    assert_eq!(
        i64_val(&session, "select count(id) as c from travel_trips").await,
        100
    );
    assert_eq!(
        i64_val(&session, "select count(empty_i) as c from travel_trips").await,
        0
    );

    // MIN/MAX from page bounds — Int64 and Float64.
    assert_eq!(
        i64_val(&session, "select min(departure) as m from travel_trips").await,
        1_700_000_000
    );
    assert_eq!(
        i64_val(&session, "select max(departure) as m from travel_trips").await,
        1_700_000_099
    );
    assert!(
        (f64_val(&session, "select min(cost) as m from travel_trips").await - 10.0).abs() < 1e-9
    );
    assert!(
        (f64_val(&session, "select max(cost) as m from travel_trips").await - 109.0).abs() < 1e-9
    );

    // MIN/MAX over a wholly-NULL column ⇒ SQL NULL.
    let nb = session
        .run("select min(empty_i) as m from travel_trips")
        .await
        .unwrap();
    assert!(
        nb[0].column(0).is_null(0),
        "MIN over an all-NULL column must be SQL NULL"
    );
}

/// Phase 7.1c (P7c): COUNT(DISTINCT col) over a bitmap-indexed column is served
/// from the bitmap's distinct-key count. A column without a bitmap index falls
/// back to DataFusion (still correct).
#[tokio::test]
async fn count_distinct_from_bitmap_partition() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // destination cycles through 5 cities ⇒ 5 distinct; bitmap-indexed.
    let rows: Vec<Vec<(u16, Value)>> = (0..100i64)
        .map(|i| {
            vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("City{}", i % 5).into_bytes())),
                (3, Value::Int64(1_700_000_000 + i)),
                (4, Value::Float64(1.0 + i as f64)),
                (5, Value::Float64(2.0)),
            ]
        })
        .collect();
    db.bulk_load(rows).unwrap();
    let session = MongrelSession::new(db);
    session.register("travel_trips").await.unwrap();

    // Bitmap-indexed column ⇒ served from bitmap cardinality.
    assert_eq!(
        i64_val(
            &session,
            "select count(distinct destination) as c from travel_trips"
        )
        .await,
        5
    );
    // PK column (no bitmap) ⇒ DataFusion fallback, still exact: 100 distinct ids.
    assert_eq!(
        i64_val(&session, "select count(distinct id) as c from travel_trips").await,
        100
    );
}

/// Regression: COUNT(col) on the visible-rows scan path (data in the mutable-run
/// overlay, not a sorted run) must exclude rows where the column is absent via
/// schema evolution — those read NULL and `COUNT(col)` excludes NULLs.
#[tokio::test]
async fn count_col_excludes_schema_evolution_nulls_on_scan() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    for i in 0..40i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("City{i}").into_bytes())),
            (3, Value::Int64(1_700_000_000 + i)),
            (4, Value::Float64(10.0 + i as f64)),
            (5, Value::Float64(2.0)),
        ])
        .unwrap();
    }
    db.commit().unwrap();
    db.flush().unwrap(); // rows live in the mutable-run overlay (not a sorted run)
    db.add_column(
        "empty_i",
        TypeId::Int64,
        ColumnFlags::empty().with(ColumnFlags::NULLABLE),
    )
    .unwrap();
    let session = MongrelSession::new(db);
    session.register("travel_trips").await.unwrap();

    // All 40 rows predate empty_i ⇒ NULL ⇒ excluded by COUNT(col).
    assert_eq!(
        i64_val(&session, "select count(empty_i) as c from travel_trips").await,
        0
    );
    // COUNT(*) still counts every row.
    assert_eq!(
        i64_val(&session, "select count(*) as c from travel_trips").await,
        40
    );
}

/// Phase 7.1: verify DataFusion's `AggregateStatistics` rewrite actually fires —
/// `COUNT(*)` over an insert-only table must be answered from metadata, so the
/// physical plan must NOT contain a `MongrelScanExec` (it becomes a constant).
#[tokio::test]
async fn metadata_aggregates_skip_the_scan() {
    let (_dir, session) = setup().await;
    let explained = session
        .run("explain select count(*) as n from travel_trips")
        .await
        .unwrap();
    // The EXPLAIN output is a single Utf8 column of plan lines.
    let plan: String = (0..explained[0].num_rows())
        .flat_map(|r| {
            explained[0]
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(r)
                .lines()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        !plan.contains("MongrelScanExec"),
        "COUNT(*) should be served from stats, not a scan. Plan:\n{plan}"
    );
}

/// Phase 7.2: native vectorized aggregates over Int64/Float64 columns — SUM/MIN/
/// MAX/AVG/COUNT — computed in one pass over the page-pruned cursor, no Arrow
/// materialization of the input. Both unfiltered and `WHERE`-filtered shapes.
#[tokio::test]
async fn native_aggregates_sum_min_max_avg_count() {
    let (_dir, session) = setup().await; // travel_trips: id 0..99, cost = 199.99+i.

    let sum_id = i64_val(&session, "select sum(id) as s from travel_trips").await;
    assert_eq!(sum_id, (0..100i64).sum::<i64>()); // 4950

    let min_id = i64_val(&session, "select min(id) as m from travel_trips").await;
    assert_eq!(min_id, 0);
    let max_id = i64_val(&session, "select max(id) as m from travel_trips").await;
    assert_eq!(max_id, 99);
    let cnt = i64_val(&session, "select count(*) as c from travel_trips").await;
    assert_eq!(cnt, 100);
    let avg_id = f64_val(&session, "select avg(id) as a from travel_trips").await;
    assert!((avg_id - 49.5).abs() < 1e-9);

    // Float64 column: sum of cost = sum(199.99+i) for i in 0..100.
    let sum_cost = f64_val(&session, "select sum(cost) as s from travel_trips").await;
    let expected_cost: f64 = (0..100).map(|i| 199.99 + i as f64).sum();
    assert!((sum_cost - expected_cost).abs() < 1e-6);

    // WHERE-filtered: id < 10 ⇒ sum 0..9 = 45, count 10.
    let sum_filt = i64_val(
        &session,
        "select sum(id) as s from travel_trips where id < 10",
    )
    .await;
    assert_eq!(sum_filt, 45);
    let cnt_filt = i64_val(
        &session,
        "select count(*) as c from travel_trips where id < 10",
    )
    .await;
    assert_eq!(cnt_filt, 10);
}

async fn i64_val(session: &MongrelSession, sql: &str) -> i64 {
    let b = session.run(sql).await.unwrap();
    b[0].column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

async fn f64_val(session: &MongrelSession, sql: &str) -> f64 {
    let b = session.run(sql).await.unwrap();
    b[0].column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap()
        .value(0)
}

/// Phase 7.2 review fix: a LIKE filter (FM-index substring superset, not exact)
/// must NOT be served by the native aggregate path — it would return the
/// superset count with no wildcard re-application. Verify COUNT(*) with a LIKE
/// WHERE is answered by DataFusion (exact wildcard semantics).
#[tokio::test]
async fn native_aggregate_rejects_like_filter() {
    let (_dir, session) = setup().await; // destination = City0..City99, FM-indexed.
                                         // LIKE '%City_1%' matches City11, City21, …, City91 (_ = any single char) ⇒ 9.
    let n = session
        .run("select count(*) as c from travel_trips where destination like '%City_1%'")
        .await
        .unwrap();
    let c = n[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(c, 9, "LIKE wildcard semantics must be applied exactly");
}
