use mongreldb_core::{
    ColumnDef, ColumnFlags, Database, MongrelError, OwnedRow, RowId, Schema, Table, TypeId,
    UpsertAction, UpsertActionKind, Value,
};
use tempfile::tempdir;

fn users_schema() -> Schema {
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

fn identity_schema() -> Schema {
    Schema {
        schema_id: 2,
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

fn row(id: i64, name: &[u8]) -> Vec<(u16, Value)> {
    vec![(1, Value::Int64(id)), (2, Value::Bytes(name.to_vec()))]
}

fn row_id_for(db: &Database, table: &str, id: i64) -> RowId {
    db.table(table)
        .unwrap()
        .lock()
        .lookup_pk(&Value::Int64(id).encode_key())
        .unwrap()
}

fn cell(row: &OwnedRow, column_id: u16) -> Value {
    row.columns
        .iter()
        .find(|(id, _)| *id == column_id)
        .map(|(_, value)| value.clone())
        .unwrap()
}

fn assert_conflict(err: MongrelError) {
    assert!(
        matches!(err, MongrelError::Conflict(_)),
        "expected conflict, got {err:?}"
    );
}

#[test]
fn put_returning_surfaces_auto_increment_post_image() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", identity_schema()).unwrap();

    let mut tx = db.begin();
    let first = tx
        .put_returning("users", vec![(2, Value::Bytes(b"alice".to_vec()))])
        .unwrap();
    let second = tx
        .put_returning("users", vec![(2, Value::Bytes(b"bob".to_vec()))])
        .unwrap();
    assert_eq!(first.auto_inc, Some(1));
    assert_eq!(cell(&first.row, 1), Value::Int64(1));
    assert_eq!(second.auto_inc, Some(2));
    assert_eq!(cell(&second.row, 1), Value::Int64(2));
    tx.commit().unwrap();

    assert_eq!(db.table("users").unwrap().lock().count(), 2);
}

#[test]
fn upsert_do_nothing_and_update_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.transaction(|tx| {
            tx.put("users", row(1, b"alice"))?;
            Ok(())
        })
        .unwrap();

        let mut tx = db.begin();
        let unchanged = tx
            .upsert("users", row(1, b"ignored"), UpsertAction::DoNothing)
            .unwrap();
        assert_eq!(unchanged.action, UpsertActionKind::Unchanged);
        assert_eq!(cell(&unchanged.row, 2), Value::Bytes(b"alice".to_vec()));
        tx.commit().unwrap();
        assert_eq!(db.table("users").unwrap().lock().count(), 1);

        let mut tx = db.begin();
        let updated = tx
            .upsert(
                "users",
                row(1, b"ignored"),
                UpsertAction::DoUpdate(vec![(2, Value::Bytes(b"ann".to_vec()))]),
            )
            .unwrap();
        assert_eq!(updated.action, UpsertActionKind::Updated);
        assert_eq!(cell(&updated.row, 2), Value::Bytes(b"ann".to_vec()));
        tx.commit().unwrap();

        let mut tx = db.begin();
        let inserted = tx
            .upsert("users", row(2, b"bob"), UpsertAction::DoNothing)
            .unwrap();
        assert_eq!(inserted.action, UpsertActionKind::Inserted);
        tx.commit().unwrap();
    }

    let reopened = Database::open(dir.path()).unwrap();
    let table = reopened.table("users").unwrap();
    let guard = table.lock();
    assert_eq!(guard.count(), 2);
    let row = guard
        .get(
            guard.lookup_pk(&Value::Int64(1).encode_key()).unwrap(),
            guard.snapshot(),
        )
        .unwrap();
    assert_eq!(row.columns.get(&2), Some(&Value::Bytes(b"ann".to_vec())));
}

#[test]
fn upsert_do_update_identical_patch_is_unchanged() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.transaction(|tx| {
        tx.put("users", row(1, b"alice"))?;
        Ok(())
    })
    .unwrap();

    let mut tx = db.begin();
    let unchanged = tx
        .upsert(
            "users",
            row(1, b"alice"),
            UpsertAction::DoUpdate(vec![(2, Value::Bytes(b"alice".to_vec()))]),
        )
        .unwrap();
    assert_eq!(unchanged.action, UpsertActionKind::Unchanged);
    assert_eq!(cell(&unchanged.row, 2), Value::Bytes(b"alice".to_vec()));
    tx.commit().unwrap();

    let table = db.table("users").unwrap();
    let guard = table.lock();
    let r = guard
        .get(
            guard.lookup_pk(&Value::Int64(1).encode_key()).unwrap(),
            guard.snapshot(),
        )
        .unwrap();
    assert_eq!(r.columns.get(&2), Some(&Value::Bytes(b"alice".to_vec())));
}

#[test]
fn update_many_and_delete_many_return_images_and_survive_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.transaction(|tx| {
            tx.put("users", row(1, b"alice"))?;
            tx.put("users", row(2, b"bob"))?;
            tx.put("users", row(3, b"cara"))?;
            Ok(())
        })
        .unwrap();

        let rid1 = row_id_for(&db, "users", 1);
        let rid2 = row_id_for(&db, "users", 2);
        let mut tx = db.begin();
        let post = tx
            .update_many(
                "users",
                vec![
                    (rid1, vec![(2, Value::Bytes(b"ann".to_vec()))]),
                    (rid2, vec![(2, Value::Bytes(b"ben".to_vec()))]),
                ],
            )
            .unwrap();
        assert_eq!(post.len(), 2);
        assert_eq!(cell(&post[0], 2), Value::Bytes(b"ann".to_vec()));
        assert_eq!(cell(&post[1], 2), Value::Bytes(b"ben".to_vec()));
        tx.commit().unwrap();

        let rid2 = row_id_for(&db, "users", 2);
        let rid3 = row_id_for(&db, "users", 3);
        let mut tx = db.begin();
        let deleted = tx.delete_many("users", vec![rid2, rid3]).unwrap();
        assert_eq!(deleted.len(), 2);
        assert_eq!(cell(&deleted[0], 2), Value::Bytes(b"ben".to_vec()));
        assert_eq!(cell(&deleted[1], 2), Value::Bytes(b"cara".to_vec()));
        tx.commit().unwrap();
        assert_eq!(db.table("users").unwrap().lock().count(), 1);
    }

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("users").unwrap().lock().count(), 1);
}

#[test]
fn transaction_truncate_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("users", users_schema()).unwrap();
        db.transaction(|tx| {
            tx.put("users", row(1, b"alice"))?;
            tx.put("users", row(2, b"bob"))?;
            Ok(())
        })
        .unwrap();
        assert_eq!(db.table("users").unwrap().lock().count(), 2);

        db.transaction(|tx| tx.truncate("users")).unwrap();
        assert_eq!(db.table("users").unwrap().lock().count(), 0);
    }

    let reopened = Database::open(dir.path()).unwrap();
    assert_eq!(reopened.table("users").unwrap().lock().count(), 0);
}

#[test]
fn table_truncate_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let table_dir = dir.path().join("table");
        let mut table = Table::create(&table_dir, users_schema(), 1).unwrap();
        table.put(row(1, b"alice")).unwrap();
        table.put(row(2, b"bob")).unwrap();
        table.commit().unwrap();
        assert_eq!(table.count(), 2);

        table.truncate().unwrap();
        table.commit().unwrap();
        assert_eq!(table.count(), 0);
    }

    let table = Table::open(dir.path().join("table")).unwrap();
    assert_eq!(table.count(), 0);
}

#[test]
fn table_delete_returning_returns_pre_image() {
    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("table");
    let mut table = Table::create(&table_dir, users_schema(), 1).unwrap();
    let row_id = table.put(row(1, b"alice")).unwrap();
    table.commit().unwrap();

    let pre = table.delete_returning(row_id).unwrap().unwrap();
    assert_eq!(cell(&pre, 2), Value::Bytes(b"alice".to_vec()));
    table.commit().unwrap();
    assert_eq!(table.count(), 0);
}

#[test]
fn truncate_conflicts_with_concurrent_put_when_truncate_wins() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.transaction(|tx| {
        tx.put("users", row(1, b"alice"))?;
        Ok(())
    })
    .unwrap();

    let mut truncate = db.begin();
    let mut put = db.begin();
    truncate.truncate("users").unwrap();
    put.put("users", row(2, b"bob")).unwrap();

    truncate.commit().unwrap();
    assert_conflict(put.commit().unwrap_err());
}

#[test]
fn truncate_conflicts_with_concurrent_put_when_put_wins() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();
    db.transaction(|tx| {
        tx.put("users", row(1, b"alice"))?;
        Ok(())
    })
    .unwrap();

    let mut truncate = db.begin();
    let mut put = db.begin();
    truncate.truncate("users").unwrap();
    put.put("users", row(2, b"bob")).unwrap();

    put.commit().unwrap();
    assert_conflict(truncate.commit().unwrap_err());
}

#[test]
fn transaction_truncate_rejects_same_table_writes() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();

    let mut tx = db.begin();
    tx.put("users", row(1, b"alice")).unwrap();
    assert!(tx.truncate("users").is_err());

    let mut tx = db.begin();
    tx.truncate("users").unwrap();
    assert!(tx.delete("users", RowId(1)).is_err());
}

#[test]
fn upsert_update_conflicts_with_concurrent_delete() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("users", users_schema()).unwrap();

    // Seed a row.
    {
        let t = db.table("users").unwrap();
        let mut g = t.lock();
        g.put(row(1, b"alice")).unwrap();
        g.commit().unwrap();
    }

    // Upsert commits first, delete loses.
    {
        let mut upsert = db.begin();
        let mut delete = db.begin();
        upsert
            .upsert(
                "users",
                row(1, b"alice"),
                UpsertAction::DoUpdate(vec![(2, Value::Bytes(b"updated".to_vec()))]),
            )
            .unwrap();
        delete
            .delete_many("users", vec![row_id_for(&db, "users", 1)])
            .unwrap();
        upsert.commit().unwrap();
        assert_conflict(delete.commit().unwrap_err());
    }

    // Re-seed, then delete commits first, upsert loses.
    {
        let t = db.table("users").unwrap();
        let mut g = t.lock();
        g.put(row(1, b"alice")).unwrap();
        g.commit().unwrap();
    }
    {
        let mut upsert = db.begin();
        let mut delete = db.begin();
        delete
            .delete_many("users", vec![row_id_for(&db, "users", 1)])
            .unwrap();
        upsert
            .upsert(
                "users",
                row(1, b"alice"),
                UpsertAction::DoUpdate(vec![(2, Value::Bytes(b"updated2".to_vec()))]),
            )
            .unwrap();
        delete.commit().unwrap();
        assert_conflict(upsert.commit().unwrap_err());
    }
}

#[test]
fn table_truncate_private_wal_deferred_until_commit() {
    use mongreldb_core::manifest;

    let dir = tempdir().unwrap();
    let table_dir = dir.path().join("table");
    {
        let mut table = Table::create(&table_dir, users_schema(), 1).unwrap();
        // Force every row to spill to an immutable sorted run so the manifest
        // holds a reference to a run file. An uncommitted truncate must not
        // delete that run or clear the manifest.
        table.set_mutable_run_spill_bytes(1);
        table.put(row(1, b"alice")).unwrap();
        table.commit().unwrap();
        table.flush().unwrap();
        assert_eq!(table.count(), 1);

        // Stage a truncate but do not commit. Dropping the table should roll
        // back the uncommitted truncate.
        table.truncate().unwrap();
    }

    let table = Table::open(&table_dir).unwrap();
    assert_eq!(table.count(), 1);
    assert!(
        table.lookup_pk(&Value::Int64(1).encode_key()).is_some(),
        "row with pk=1 should survive an uncommitted truncate"
    );

    let manifest = manifest::read(&table_dir, None).unwrap();
    assert!(
        !manifest.runs.is_empty(),
        "manifest should still reference the flushed run"
    );
    for run in &manifest.runs {
        let path = table_dir.join("_runs").join(format!("r-{}.sr", run.run_id));
        assert!(
            path.exists(),
            "run file referenced by manifest should still exist: {:?}",
            path
        );
    }
}
