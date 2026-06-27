//! P1.4 — multi-table `Database` shares one epoch clock, caches, and snapshot
//! registry across tables; reopen sees every table.

use mongreldb_core::{schema::*, Database, Epoch, Value};
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

fn orders_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: vec![],
        colocation: vec![],
    }
}

fn items_schema() -> Schema {
    Schema {
        schema_id: 2,
        columns: vec![ColumnDef {
            id: 1,
            name: "sku".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: vec![],
        colocation: vec![],
    }
}

#[test]
fn database_creates_tables_and_shares_one_clock() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let _o = db.create_table("orders", orders_schema()).unwrap();
    let _i = db.create_table("items", items_schema()).unwrap();
    assert_eq!(db.table_names().len(), 2);

    let e_pre = db.snapshot().0.epoch;
    db.table("orders")
        .unwrap()
        .lock()
        .put(vec![(1, Value::Int64(1))])
        .unwrap();
    let e_orders = db.table("orders").unwrap().lock().commit().unwrap();
    db.table("items")
        .unwrap()
        .lock()
        .put(vec![(1, Value::Int64(2))])
        .unwrap();
    let e_items = db.table("items").unwrap().lock().commit().unwrap();
    assert!(e_orders.0 > e_pre.0);
    assert!(
        e_items.0 > e_orders.0,
        "items epoch must exceed orders epoch"
    );

    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table_names().len(), 2);
    let _t = db.table("orders").unwrap();
    let _t = db.table("items").unwrap();
}

#[test]
fn database_drop_table_removes_it() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", orders_schema()).unwrap();
    db.create_table("b", items_schema()).unwrap();
    assert_eq!(db.table_names().len(), 2);
    db.drop_table("a").unwrap();
    assert_eq!(db.table_names().len(), 1);
    assert!(db.table("a").is_err());
    // reopen still sees only b
    drop(db);
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table_names().len(), 1);
}

#[test]
fn database_snapshot_is_retained() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("orders", orders_schema()).unwrap();
    db.table("orders")
        .unwrap()
        .lock()
        .put(vec![(1, Value::Int64(1))])
        .unwrap();
    db.table("orders").unwrap().lock().commit().unwrap();
    let (snap, _guard) = db.snapshot();
    // create_table (DDL) advances the epoch to 1, then the put+commit to 2.
    assert_eq!(snap.epoch, Epoch(2));
}

#[test]
fn concurrent_cross_table_commits_publish_in_order() {
    // Two tables committing concurrently on the shared epoch authority must
    // never publish `visible` past an unpublished lower epoch: every committed
    // epoch is durable and visible before a higher one becomes visible.
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("a", orders_schema()).unwrap();
    db.create_table("b", items_schema()).unwrap();

    let n = 200usize;
    let mut handles = Vec::new();
    for name in ["a", "b"] {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..n {
                let h = db.table(name).unwrap();
                let mut t = h.lock();
                t.put(vec![(1, Value::Int64(i as i64))]).unwrap();
                let committed = t.commit().unwrap();
                let vis = db.visible_epoch();
                assert!(
                    vis.0 >= committed.0,
                    "visible {vis:?} < committed {committed:?} (unpublished)"
                );
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    // 2 create_table DDL commits + 2*n data commits advance the shared clock.
    assert_eq!(db.visible_epoch(), Epoch(2 + (2 * n) as u64));
}
