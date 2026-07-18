//! Phase 8.3: incremental aggregate-cache maintenance.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{
    AggState, Condition, Database, NativeAgg, Permission, PolicyCommand, RowPolicy,
    SecurityCatalog, SecurityExpr, Table, Value,
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
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "category".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "value".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "cat_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn put_range(db: &mut Table, lo: i64, hi: i64) {
    for i in lo..hi {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Int64(i % 10)),
            (3, Value::Int64(i * 2 + 1)),
        ])
        .unwrap();
    }
}

fn put_range_database(db: &Database, lo: i64, hi: i64) {
    let mut transaction = db.begin();
    for i in lo..hi {
        transaction
            .put(
                "items",
                vec![
                    (1, Value::Int64(i)),
                    (2, Value::Int64(i % 10)),
                    (3, Value::Int64(i * 2 + 1)),
                ],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    db.table("items").unwrap().lock().flush().unwrap();
}

#[test]
fn database_wrapper_reuses_incremental_delta_cache() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("items", schema()).unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    put_range_database(&db, 0, 100);

    let cold = db
        .incremental_aggregate_for_current_principal("items", &[], Some(3), NativeAgg::Sum)
        .unwrap();
    assert!(!cold.incremental);
    assert_eq!(cold.state.point(), Some(10_000.0));

    put_range_database(&db, 100, 150);
    let warm = db
        .incremental_aggregate_for_current_principal("items", &[], Some(3), NativeAgg::Sum)
        .unwrap();
    assert!(warm.incremental);
    assert_eq!(warm.delta_rows, 50);
    assert_eq!(warm.state.point(), Some(22_500.0));
}

#[test]
fn database_wrapper_invalidates_on_security_change_and_rejects_rls() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "pw").unwrap();
    admin.create_table("items", schema()).unwrap();
    admin
        .table("items")
        .unwrap()
        .lock()
        .set_mutable_run_spill_bytes(1);
    put_range_database(&admin, 0, 100);
    admin.create_user("alice", "pw").unwrap();
    admin.create_role("reader").unwrap();
    admin
        .grant_permission(
            "reader",
            Permission::Select {
                table: "items".into(),
            },
        )
        .unwrap();
    admin.grant_role("alice", "reader").unwrap();
    let alice = admin.resolve_principal("alice").unwrap();

    let cold = admin
        .incremental_aggregate_for_principal(
            "items",
            &[],
            None,
            NativeAgg::Count,
            Some(&alice),
            true,
        )
        .unwrap();
    assert!(!cold.incremental);
    let warm = admin
        .incremental_aggregate_for_principal(
            "items",
            &[],
            None,
            NativeAgg::Count,
            Some(&alice),
            true,
        )
        .unwrap();
    assert!(warm.incremental);

    admin.create_role("extra").unwrap();
    admin.grant_role("alice", "extra").unwrap();
    let invalidated = admin
        .incremental_aggregate_for_principal(
            "items",
            &[],
            None,
            NativeAgg::Count,
            Some(&alice),
            true,
        )
        .unwrap();
    assert!(!invalidated.incremental);

    admin
        .set_security_catalog(SecurityCatalog {
            rls_tables: vec!["items".into()],
            policies: vec![RowPolicy {
                name: "visible".into(),
                table: "items".into(),
                command: PolicyCommand::Select,
                subjects: vec!["public".into()],
                permissive: true,
                using: Some(SecurityExpr::True),
                with_check: None,
            }],
            masks: Vec::new(),
        })
        .unwrap();
    assert!(matches!(
        admin.incremental_aggregate_for_principal(
            "items",
            &[],
            None,
            NativeAgg::Count,
            Some(&alice),
            true,
        ),
        Err(mongreldb_core::MongrelError::InvalidArgument(message))
            if message.contains("unsupported while RLS or column masks are active")
    ));
}

#[test]
fn cold_then_warm_matches_exact() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill per flush so the incremental watermark holds
    put_range(&mut db, 0, 1000);
    db.flush().unwrap();

    let exact_sum = |lo: i64, hi: i64| -> f64 { (lo..hi).map(|i| (i * 2 + 1) as f64).sum::<f64>() };

    // First call: cold ⇒ full recompute.
    let r1 = db
        .aggregate_incremental(42, &[], Some(3), NativeAgg::Sum)
        .unwrap();
    assert!(!r1.incremental, "first call is a cold full recompute");
    assert_eq!(r1.state.point(), Some(exact_sum(0, 1000)));

    // Append 500 rows + flush ⇒ new epoch.
    put_range(&mut db, 1000, 1500);
    db.flush().unwrap();

    // Second call: warm ⇒ incremental delta merge.
    let r2 = db
        .aggregate_incremental(42, &[], Some(3), NativeAgg::Sum)
        .unwrap();
    assert!(r2.incremental, "second call should hit the warm cache");
    assert_eq!(r2.delta_rows, 500, "only the 500 new rows are processed");
    assert_eq!(r2.state.point(), Some(exact_sum(0, 1500)));

    // A third append + query stays incremental.
    put_range(&mut db, 1500, 1700);
    db.flush().unwrap();
    let r3 = db
        .aggregate_incremental(42, &[], Some(3), NativeAgg::Sum)
        .unwrap();
    assert!(r3.incremental);
    assert_eq!(r3.delta_rows, 200);
    assert_eq!(r3.state.point(), Some(exact_sum(0, 1700)));
}

#[test]
fn incremental_count_and_avg_with_filter() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill per flush so the incremental watermark holds
    put_range(&mut db, 0, 1000);
    db.flush().unwrap();

    let cat0 = [Condition::BitmapEq {
        column_id: 2,
        value: Value::Int64(0).encode_key(),
    }];
    // category 0 in [0,1000): i = 0,10,…,990 ⇒ 100 rows.

    let rc = db
        .aggregate_incremental(7, &cat0, None, NativeAgg::Count)
        .unwrap();
    assert_eq!(rc.state, AggState::Count(100));

    put_range(&mut db, 1000, 2000);
    db.flush().unwrap();
    // category 0 in [1000,2000): 100 more ⇒ 200 total.
    let rc2 = db
        .aggregate_incremental(7, &cat0, None, NativeAgg::Count)
        .unwrap();
    assert!(rc2.incremental);
    assert_eq!(rc2.state, AggState::Count(200));

    // AVG of value for category 0.
    let avg_now = db
        .aggregate_incremental(8, &cat0, Some(3), NativeAgg::Avg)
        .unwrap();
    let exact_avg = ((0..2000)
        .step_by(10)
        .map(|i| (i * 2 + 1) as f64)
        .sum::<f64>())
        / 200.0;
    assert!(
        (avg_now.state.point().unwrap() - exact_avg).abs() < 1e-9,
        "incremental avg matches exact"
    );
}

#[test]
fn delete_disables_incremental() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill per flush so the incremental watermark holds
    put_range(&mut db, 0, 1000);
    db.flush().unwrap();

    // Warm the cache.
    let _ = db
        .aggregate_incremental(1, &[], None, NativeAgg::Count)
        .unwrap();

    // A delete invalidates append-only semantics.
    db.delete(mongreldb_core::RowId(0)).unwrap();
    db.flush().unwrap();

    let r = db
        .aggregate_incremental(1, &[], None, NativeAgg::Count)
        .unwrap();
    assert!(!r.incremental, "a delete forces a full recompute");
    assert_eq!(r.state, AggState::Count(999));
}

#[test]
fn distinct_keys_are_independent() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill per flush so the incremental watermark holds
    put_range(&mut db, 0, 1000);
    db.flush().unwrap();

    let a = db
        .aggregate_incremental(1, &[], Some(3), NativeAgg::Sum)
        .unwrap();
    let b = db
        .aggregate_incremental(2, &[], Some(3), NativeAgg::Count)
        .unwrap();
    assert!(!a.incremental && !b.incremental);

    put_range(&mut db, 1000, 1500);
    db.flush().unwrap();
    let a2 = db
        .aggregate_incremental(1, &[], Some(3), NativeAgg::Sum)
        .unwrap();
    let b2 = db
        .aggregate_incremental(2, &[], Some(3), NativeAgg::Count)
        .unwrap();
    assert!(a2.incremental && b2.incremental, "both caches stay warm");
    assert_eq!(b2.state, AggState::Count(1500));
}

#[test]
fn count_column_excludes_nulls() {
    // Regression (Phase 8 review): COUNT(col) must skip NULL cells, unlike
    // COUNT(*).
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill per flush so the incremental watermark holds
    for i in 0..10i64 {
        let mut cols = vec![(1, Value::Int64(i)), (2, Value::Int64(i % 10))];
        if i % 2 == 1 {
            cols.push((3, Value::Int64(i * 2 + 1)));
        } // even ids ⇒ value is NULL (absent)
        db.put(cols).unwrap();
    }
    db.flush().unwrap();

    let count_star = db
        .aggregate_incremental(11, &[], None, NativeAgg::Count)
        .unwrap();
    assert_eq!(count_star.state, AggState::Count(10));
    let count_col = db
        .aggregate_incremental(12, &[], Some(3), NativeAgg::Count)
        .unwrap();
    assert_eq!(count_col.state, AggState::Count(5), "NULL values excluded");
}

#[test]
fn uncommitted_writes_do_not_poison_cache() {
    // Regression (Phase 8 review): pending (uncommitted) writes must not seed a
    // watermark that later skips just-committed rows. `put` without `flush`
    // leaves the memtable non-empty ⇒ no incremental seeding.
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill per flush so the incremental watermark holds
    put_range(&mut db, 0, 100);
    db.flush().unwrap(); // warm the cache at epoch E1
    let _ = db
        .aggregate_incremental(21, &[], Some(3), NativeAgg::Sum)
        .unwrap();

    put_range(&mut db, 100, 150); // pending writes, no flush
    db.flush().unwrap(); // commit + flush ⇒ epoch E2, memtable empty
    let r = db
        .aggregate_incremental(21, &[], Some(3), NativeAgg::Sum)
        .unwrap();
    let exact = (0..150).map(|i| (i * 2 + 1) as i128).sum::<i128>() as f64;
    assert_eq!(r.state.point(), Some(exact), "no rows skipped");
}
