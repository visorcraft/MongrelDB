use mongreldb_core::{schema::*, Database};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn copy_on_write_table_read_guards_do_not_serialize() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", schema()).unwrap();

    let handle = db.table("t").unwrap();
    let first = handle.read();
    let second_handle = handle.clone();
    let (acquired_tx, acquired_rx) = mpsc::channel();

    let reader = std::thread::spawn(move || {
        let _second = second_handle.read();
        acquired_tx.send(()).unwrap();
    });

    acquired_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("a second read guard must acquire while the first remains held");
    drop(first);
    reader.join().unwrap();
}
