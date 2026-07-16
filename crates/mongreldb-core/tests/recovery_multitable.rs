//! P2.6 — two-pass, epoch-gated, no-truncate multi-table recovery.

use mongreldb_core::{
    schema::*,
    wal::{AddedRun, Op, SharedWal},
    Database, Epoch, MongrelError, Row, RowId, Value,
};
use tempfile::tempdir;

fn one_int_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
        }],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn committed_put_decode_failure_is_wal_corruption() {
    let dir = tempdir().unwrap();
    let epoch = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", one_int_schema()).unwrap();
        db.visible_epoch().0 + 1
    };
    let mut wal = SharedWal::open(dir.path(), Epoch(epoch - 1), None).unwrap();
    wal.append(
        999,
        0,
        Op::Put {
            table_id: 0,
            rows: vec![0xff, 0x00, 0xff],
        },
    )
    .unwrap();
    wal.append_commit(999, Epoch(epoch), &[]).unwrap();
    wal.group_sync().unwrap();
    drop(wal);

    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::CorruptWal { .. })
    ));
}

#[test]
fn committed_write_to_unknown_table_is_wal_corruption() {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    let mut wal = SharedWal::open(dir.path(), Epoch(0), None).unwrap();
    let rows = bincode::serialize(&vec![Row::new(RowId(1), Epoch(1))]).unwrap();
    wal.append(
        91,
        999,
        Op::Put {
            table_id: 999,
            rows,
        },
    )
    .unwrap();
    wal.append_commit(91, Epoch(1), &[]).unwrap();
    wal.group_sync().unwrap();
    drop(wal);

    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::CorruptWal { .. })
    ));
}

#[test]
fn committed_write_at_or_after_drop_epoch_is_wal_corruption() {
    let dir = tempdir().unwrap();
    let drop_epoch = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", one_int_schema()).unwrap();
        db.drop_table_with_epoch("items").unwrap().0
    };
    let mut wal = SharedWal::open(dir.path(), Epoch(drop_epoch), None).unwrap();
    let rows = bincode::serialize(&vec![Row::new(RowId(1), Epoch(drop_epoch + 1))]).unwrap();
    wal.append(92, 0, Op::Put { table_id: 0, rows }).unwrap();
    wal.append_commit(92, Epoch(drop_epoch + 1), &[]).unwrap();
    wal.group_sync().unwrap();
    drop(wal);

    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::CorruptWal { .. })
    ));
}

#[test]
fn committed_spilled_run_missing_from_pending_and_final_is_corruption() {
    let dir = tempdir().unwrap();
    let epoch = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", one_int_schema()).unwrap();
        db.visible_epoch().0 + 1
    };
    let mut wal = SharedWal::open(dir.path(), Epoch(epoch - 1), None).unwrap();
    wal.append_commit(
        1000,
        Epoch(epoch),
        &[AddedRun {
            table_id: 0,
            run_id: 4242,
            row_count: 1,
            level: 0,
            min_row_id: 1,
            max_row_id: 1,
            content_hash: [0; 32],
        }],
    )
    .unwrap();
    wal.group_sync().unwrap();
    drop(wal);

    assert!(matches!(
        Database::open(dir.path()),
        Err(MongrelError::CorruptWal { .. })
    ));
}

#[test]
fn recovery_replays_committed_skips_uncommitted_and_gates_by_flushed_epoch() {
    let dir = tempdir().unwrap();

    // 1) Commit transactions across 2 tables, then drop the DB without any
    //    clean shutdown (a raw reopen simulates a crash).
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", one_int_schema()).unwrap();
        db.create_table("b", one_int_schema()).unwrap();
        db.transaction(|t| {
            t.put("a", vec![(1, Value::Int64(1))])?;
            t.put("b", vec![(1, Value::Int64(2))])?;
            Ok(())
        })
        .unwrap();
        db.transaction(|t| {
            t.put("a", vec![(1, Value::Int64(3))])?;
            Ok(())
        })
        .unwrap();
    }

    // 2) Reopen: all committed rows present.
    {
        let db = Database::open(dir.path()).unwrap();
        assert_eq!(db.table("a").unwrap().lock().count(), 2);
        assert_eq!(db.table("b").unwrap().lock().count(), 1);
    }

    // 3) flush table A (flushed_epoch advances), append+commit a later txn to
    //    A, reopen -> the later txn is still applied (gated by epoch, not seq).
    {
        let db = Database::open(dir.path()).unwrap();
        // Flush A so its data is durable in a run and flushed_epoch advances.
        db.table("a").unwrap().lock().flush().unwrap();
        let pre = db.visible_epoch().0;
        db.transaction(|t| {
            t.put("a", vec![(1, Value::Int64(7))])?;
            Ok(())
        })
        .unwrap();
        let post = db.visible_epoch().0;
        assert!(post > pre, "commit must advance the epoch");
        assert_eq!(db.table("a").unwrap().lock().count(), 3);
    }
    {
        let db = Database::open(dir.path()).unwrap();
        // The post-flush txn (epoch > flushed_epoch) is replayed from the WAL.
        assert_eq!(db.table("a").unwrap().lock().count(), 3);
        assert_eq!(db.table("b").unwrap().lock().count(), 1);
    }
}

#[test]
fn recovery_preserves_batched_update_delete_then_put_order() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("items", one_int_schema()).unwrap();
        db.transaction(|transaction| {
            transaction.put("items", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();
        let row_id = db
            .table("items")
            .unwrap()
            .lock()
            .visible_rows(db.snapshot().0)
            .unwrap()[0]
            .row_id;
        db.transaction(|transaction| {
            transaction.update_many("items", vec![(row_id, vec![(1, Value::Int64(2))])])?;
            Ok(())
        })
        .unwrap();
    }

    let db = Database::open(dir.path()).unwrap();
    let rows = db
        .table("items")
        .unwrap()
        .lock()
        .visible_rows(db.snapshot().0)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(2)));
}

#[test]
fn recovery_ignores_uncommitted_txn() {
    // A transaction whose records were fsync'd but whose TxnCommit was NOT
    // (crash mid-commit) must not appear after reopen. We simulate this by
    // staging puts then dropping without commit (rollback).
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", one_int_schema()).unwrap();
        let mut tx = db.begin();
        tx.put("a", vec![(1, Value::Int64(99))]).unwrap();
        // rollback (drop) — nothing committed
        tx.rollback();
    }
    let db = Database::open(dir.path()).unwrap();
    assert_eq!(db.table("a").unwrap().lock().count(), 0);
}

#[test]
fn txn_ids_do_not_alias_across_reopen() {
    // open gen=0 writes txn_ids in generation 0; reopen bumps the generation so
    // new txn_ids cannot collide with any pre-reopen id.
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("a", one_int_schema()).unwrap();
    let t1 = db.begin();
    let t2 = db.begin();
    // Same generation: distinct low-counter ids, same high generation bits.
    assert_ne!(t1.txn_id(), t2.txn_id());
    let gen1 = t1.txn_id() >> 32;
    let id1 = t1.txn_id();
    let id2 = t2.txn_id();
    assert_eq!(gen1, id2 >> 32);
    drop(t1);
    drop(t2);
    drop(db);

    let db = Database::open(dir.path()).unwrap();
    let t3 = db.begin();
    let gen2 = t3.txn_id() >> 32;
    // Different generation (high 32 bits advanced) — cannot equal any prior id.
    assert_ne!(gen2, gen1, "open must bump the generation");
    assert_ne!(t3.txn_id(), id1);
    assert_ne!(t3.txn_id(), id2);
}

#[test]
fn malformed_generation_sidecar_fails_closed() {
    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    std::fs::write(dir.path().join("_meta/generation"), [1_u8, 2, 3]).unwrap();
    assert!(Database::open(dir.path()).is_err());
}

#[test]
fn generation_sidecar_cannot_move_behind_retained_wal() {
    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", one_int_schema()).unwrap();
    }
    {
        let db = Database::open(dir.path()).unwrap();
        db.transaction(|transaction| {
            transaction.put("a", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();
    }
    std::fs::write(dir.path().join("_meta/generation"), 0_u64.to_le_bytes()).unwrap();
    assert!(Database::open(dir.path()).is_err());
}

#[test]
fn missing_generation_migrates_above_retained_wal() {
    let dir = tempdir().unwrap();
    let prior = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("a", one_int_schema()).unwrap();
        let txn_id = db.begin().txn_id();
        txn_id
    };
    std::fs::remove_file(dir.path().join("_meta/generation")).unwrap();
    let reopened = Database::open(dir.path()).unwrap();
    let next = reopened.begin().txn_id();
    assert!(next >> 32 > prior >> 32);
    assert_ne!(next, prior);
}

#[cfg(unix)]
#[test]
fn generation_symlinks_never_redirect_reads_or_atomic_writes() {
    use std::os::unix::fs::symlink;

    let dir = tempdir().unwrap();
    drop(Database::create(dir.path()).unwrap());
    let outside = dir.path().join("outside");
    std::fs::write(&outside, b"outside").unwrap();
    let generation = dir.path().join("_meta/generation");
    std::fs::remove_file(&generation).unwrap();
    symlink(&outside, &generation).unwrap();
    assert!(Database::open(dir.path()).is_err());
    assert_eq!(std::fs::read(&outside).unwrap(), b"outside");

    std::fs::remove_file(&generation).unwrap();
    std::fs::write(&generation, 0_u64.to_le_bytes()).unwrap();
    let obsolete_temp = dir.path().join("_meta/.generation.tmp");
    symlink(&outside, &obsolete_temp).unwrap();
    drop(Database::open(dir.path()).unwrap());
    assert_eq!(std::fs::read(&outside).unwrap(), b"outside");
    assert!(obsolete_temp
        .symlink_metadata()
        .unwrap()
        .file_type()
        .is_symlink());
}

#[cfg(unix)]
#[test]
fn generation_write_stays_on_pinned_root_after_root_rename() {
    let parent = tempdir().unwrap();
    let original = parent.path().join("database");
    drop(Database::create(&original).unwrap());
    let durable = mongreldb_core::durable_file::DurableRoot::open(&original).unwrap();
    let moved = parent.path().join("moved");
    std::fs::rename(&original, &moved).unwrap();
    std::fs::create_dir_all(original.join("_meta")).unwrap();
    std::fs::write(original.join("_meta/generation"), 9_u64.to_le_bytes()).unwrap();

    mongreldb_core::catalog::write_generation(&durable, 42).unwrap();
    assert_eq!(
        u64::from_le_bytes(
            std::fs::read(moved.join("_meta/generation"))
                .unwrap()
                .try_into()
                .unwrap()
        ),
        42
    );
    assert_eq!(
        u64::from_le_bytes(
            std::fs::read(original.join("_meta/generation"))
                .unwrap()
                .try_into()
                .unwrap()
        ),
        9
    );
}

#[test]
fn ddl_is_durable_via_wal_before_catalog_checkpoint() {
    use mongreldb_core::{DdlOp, Op, SharedWal};

    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("orders", one_int_schema()).unwrap();
        // A data commit on the new table.
        db.transaction(|t| {
            t.put("orders", vec![(1, Value::Int64(7))])?;
            Ok(())
        })
        .unwrap();
    }

    // The shared WAL carries the CreateTable Ddl record (durability does not
    // rest solely on the catalog checkpoint).
    let recs = SharedWal::replay(dir.path()).unwrap();
    assert!(
        recs.iter().any(|r| matches!(
            r.op,
            Op::Ddl(DdlOp::CreateTable { ref name, .. }) if name == "orders"
        )),
        "CreateTable must be logged to the shared WAL"
    );

    // Reopen sees the table and its data.
    let db = Database::open(dir.path()).unwrap();
    assert!(db.table_names().iter().any(|n| n == "orders"));
    assert_eq!(db.table("orders").unwrap().lock().count(), 1);
}

#[test]
fn ddl_recovered_from_wal_when_catalog_checkpoint_is_stale() {
    // Simulate a crash between WAL group-sync and the catalog checkpoint by
    // overwriting the catalog with the pre-DDL (empty) state. The table dir
    // exists on disk, but the catalog doesn't know about it. Reopen MUST
    // recover the table by replaying the committed Op::Ddl(CreateTable).
    use mongreldb_core::catalog;

    let dir = tempdir().unwrap();
    {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("recovered", one_int_schema()).unwrap();
        db.transaction(|t| {
            t.put("recovered", vec![(1, Value::Int64(42))])?;
            Ok(())
        })
        .unwrap();
    }

    // Stomp the catalog back to empty (simulating a checkpoint that never landed).
    let empty = catalog::Catalog::empty();
    catalog::write_atomic(dir.path(), &empty, None).unwrap();

    // Reopen: DDL replay should recover the table.
    let db = Database::open(dir.path()).unwrap();
    assert!(
        db.table_names().iter().any(|n| n == "recovered"),
        "table must be recovered from WAL DDL replay"
    );
    assert_eq!(db.table("recovered").unwrap().lock().count(), 1);
}

#[test]
fn ddl_recovery_completes_a_partial_table_directory() {
    use mongreldb_core::{catalog, manifest};

    let dir = tempdir().unwrap();
    let table_id = {
        let db = Database::create(dir.path()).unwrap();
        let table_id = db.create_table("recovered", one_int_schema()).unwrap();
        drop(db);
        table_id
    };

    catalog::write_atomic(dir.path(), &catalog::Catalog::empty(), None).unwrap();
    let table_dir = dir.path().join("tables").join(table_id.to_string());
    std::fs::remove_file(table_dir.join(manifest::MANIFEST_FILENAME)).unwrap();
    std::fs::remove_file(table_dir.join("schema.json")).unwrap();

    let db = Database::open(dir.path()).unwrap();
    assert!(db.table_names().iter().any(|name| name == "recovered"));
    assert!(table_dir.join(manifest::MANIFEST_FILENAME).is_file());
    assert!(table_dir.join("schema.json").is_file());
}

#[cfg(feature = "encryption")]
#[test]
fn encrypted_create_table_recovers_when_dir_missing_after_ddl_sync() {
    // Simulate a crash on an ENCRYPTED database between the shared-WAL DDL
    // group-sync and `Table::create_in`: the committed CreateTable is durable in
    // the WAL, but neither the table dir nor the catalog checkpoint landed.
    // Recovery must reconstruct the table dir with an ENCRYPTED + authenticated
    // manifest so the follow-up `Table::open_in` (which reads with the encrypted
    // meta DEK) can authenticate it. A plaintext manifest renders the table
    // permanently unopenable.
    use mongreldb_core::catalog;
    use mongreldb_core::encryption::{meta_dek_for, Kek, SALT_LEN};

    let dir = tempdir().unwrap();
    {
        let db = Database::create_encrypted(dir.path(), "pw").unwrap();
        // The DDL is group-synced to the shared WAL inside create_table. The
        // crash we model strikes between that sync and `Table::create_in`, so no
        // data could have been committed to the table yet.
        db.create_table("recovered", one_int_schema()).unwrap();
    }

    // Re-derive the DB-wide meta DEK to stomp the catalog back to empty
    // (encrypted + authenticated), simulating the checkpoint that never landed.
    let salt_bytes = std::fs::read(dir.path().join("_meta").join("keys")).unwrap();
    let mut salt = [0u8; SALT_LEN];
    salt.copy_from_slice(&salt_bytes);
    let kek = Kek::derive("pw", &salt).unwrap();
    let meta_dek = meta_dek_for(Some(&kek));

    let empty = catalog::Catalog::empty();
    catalog::write_atomic(dir.path(), &empty, meta_dek.as_ref()).unwrap();

    // Remove every table dir on disk (simulating dirs that never landed before
    // the crash).
    let tables_dir = dir.path().join("tables");
    for e in std::fs::read_dir(&tables_dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            std::fs::remove_dir_all(p).unwrap();
        }
    }

    // Reopen: DDL replay must reconstruct the table dir with an ENCRYPTED +
    // authenticated manifest. A plaintext manifest would fail to authenticate in
    // `Table::open_in` and the open would error out.
    let db = Database::open_encrypted(dir.path(), "pw").unwrap();
    assert!(
        db.table_names().iter().any(|n| n == "recovered"),
        "table must be recovered from WAL DDL replay"
    );
    // The reconstructed table must be fully usable: a commit succeeds and the
    // row is visible (proving the manifest authenticates on every subsequent
    // operation, not just the initial open).
    db.transaction(|t| {
        t.put("recovered", vec![(1, Value::Int64(42))])?;
        Ok(())
    })
    .unwrap();
    assert_eq!(db.table("recovered").unwrap().lock().count(), 1);

    // And it survives a clean reopen (manifest persisted encrypted by the
    // commit's persist_manifest).
    drop(db);
    let db = Database::open_encrypted(dir.path(), "pw").unwrap();
    assert_eq!(db.table("recovered").unwrap().lock().count(), 1);
}

#[test]
fn drop_table_recovered_from_wal_when_catalog_checkpoint_is_stale() {
    // Symmetric: if the DropTable DDL was committed to the WAL but the catalog
    // checkpoint didn't land, reopen must NOT show the table as live.
    use mongreldb_core::catalog;

    let dir = tempdir().unwrap();
    let pre_drop_catalog = {
        let db = Database::create(dir.path()).unwrap();
        db.create_table("doomed", one_int_schema()).unwrap();
        db.transaction(|t| {
            t.put("doomed", vec![(1, Value::Int64(1))])?;
            Ok(())
        })
        .unwrap();

        // Snapshot the pre-drop catalog so we can restore it (simulating the
        // checkpoint landing BEFORE the drop checkpoint).
        let cat = catalog::read(dir.path(), None).unwrap().unwrap();
        db.drop_table("doomed").unwrap();
        cat
    };

    // Restore the pre-drop catalog (stale: table still Live in catalog, but
    // DropTable is durable in the WAL).
    catalog::write_atomic(dir.path(), &pre_drop_catalog, None).unwrap();

    let db = Database::open(dir.path()).unwrap();
    assert!(
        !db.table_names().iter().any(|n| n == "doomed"),
        "DropTable must be recovered from WAL DDL replay"
    );
}
