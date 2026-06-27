//! P1.4 — multi-table `Database` shares one epoch clock, caches, and snapshot
//! registry across tables; reopen sees every table.

use mongreldb_core::{schema::*, Database, Epoch, Value};
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
    assert_eq!(snap.epoch, Epoch(1));
}
