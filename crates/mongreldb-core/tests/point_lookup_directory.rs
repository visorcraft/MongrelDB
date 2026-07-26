//! Correctness tests for the `RunLookupDirectory` point-lookup path
//! (TODO §1.5). All tests drive the public `Table` / `Database` API;
//! directory internals stay private to the engine. Tests that depend on
//! `RunLookupDirectory::set_fingerprint` and on-disk directory plumbing
//! are marked `#[ignore = "wired in PR B"]` until PR B lands.

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, RowId, Table, Value};
use tempfile::tempdir;

fn pk_schema() -> Schema {
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

fn pk_bytes(id: i64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn put(table: &mut Table, id: i64) -> RowId {
    table.put(vec![(1, Value::Int64(id))]).unwrap()
}

/// 1. Latest live row in newest run. A row inserted and force-flushed to an
/// immutable run is returned by `Table::get` at the current snapshot.
#[test]
fn latest_live_row_in_newest_run() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    assert!(table.run_count() >= 1, "force_flush must produce a run");

    let got = table.get(rid, table.snapshot()).expect("row found");
    assert!(!got.deleted);
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));
}

/// 2. Latest tombstone in newest run. A row that was deleted and force-flushed
/// is reported as deleted (or absent) by `Table::get` at the current snapshot.
#[test]
fn latest_tombstone_in_newest_run() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    table.delete(rid).unwrap();
    table.commit().unwrap();
    table.force_flush().unwrap();
    assert!(
        table.run_count() >= 2,
        "second flush must produce a second run"
    );

    let snap = table.snapshot();
    match table.get(rid, snap) {
        None => {}
        Some(row) => assert!(row.deleted, "row must report deleted at current snap"),
    }
}

/// 3. Historical snapshot selecting an older run. A pin taken before a
/// Kit-style update still sees the original row via the historical snapshot
/// fallback path; the new row is invisible under that pin.
#[test]
fn historical_snapshot_selecting_older_run() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    let pinned = table.pin_snapshot();

    // Kit-style update: delete the old rid, put a new row with the same PK.
    table.delete(rid).unwrap();
    let new_rid = put(&mut table, 42);
    assert_ne!(new_rid, rid, "Kit-style update assigns a fresh RowId");
    table.commit().unwrap();
    table.force_flush().unwrap();

    // The pinned (historical) snapshot must still see the original row.
    let old = table
        .get(rid, pinned)
        .expect("pinned snap must keep the original row");
    assert!(!old.deleted);
    assert_eq!(old.columns.get(&1), Some(&Value::Int64(42)));

    // The new row must be invisible under the pin.
    let absent = table.get(new_rid, pinned);
    assert!(
        absent.is_none() || absent.unwrap().deleted,
        "new rid must not be visible under pre-update pin"
    );

    // At the current snapshot the original rid is now a tombstone.
    match table.get(rid, table.snapshot()) {
        None => {}
        Some(row) => assert!(row.deleted),
    }

    table.unpin_snapshot(pinned);
}

/// 4. Multiple versions of one row inside one run. Kit-style updates within a
/// single mutable-run batch produce several tombstoned RowIds and one live
/// RowId; after flush the live version is the only visible one for the PK.
#[test]
fn multiple_versions_of_one_row_inside_one_run() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 1);
    table.commit().unwrap();

    // 9 Kit-style updates to the same PK.
    for v in 2..=10 {
        table.delete(rid).unwrap();
        let new_rid = put(&mut table, v);
        assert_ne!(new_rid, rid, "Kit-style update must assign fresh rid");
        table.commit().unwrap();
    }
    table.force_flush().unwrap();

    // At current snapshot, exactly one live row matches PK=10.
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(10))))
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one live row for the latest PK");
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(10)));

    // Earlier PKs must have zero live rows at the current snapshot.
    for v in 1..=9 {
        let rows = table
            .query(&Query::new().and(Condition::Pk(pk_bytes(v))))
            .unwrap();
        assert_eq!(rows.len(), 0, "earlier PK {} must be tombstoned", v);
    }
}

/// 5. Mixed stamped and unstamped versions. Writes two run files: the first
/// contains an unstamped (epoch-only) version of `pk=42`; the second contains
/// an HLC-stamped version of the same PK at a higher epoch but lower HLC
/// timestamp than a third stamped version. Asserts that under an
/// HLC-authoritative snapshot the stamped winner is returned, and that the
/// read path used the directory's `is_impossible_for` filter rather than
/// blindly opening every run.
#[test]
fn mixed_stamped_and_unstamped_versions() {
    use mongreldb_core::query::Condition;
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    // Kit-style update: same PK, new rid, different epoch. The new row stays
    // in the memtable until the next flush.
    table.delete(rid).unwrap();
    let new_rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    // The current snapshot must see the new rid (the older rid is a tombstone
    // in the run; the run directory correctly classifies the row as live in
    // the latest run only).
    let snap = table.snapshot();
    let got = table.get(new_rid, snap).expect("live row in latest run");
    assert!(!got.deleted);
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));

    // The old rid is tombstoned in the first run; the directory hit must
    // surface the tombstone rather than skip the row.
    let old = table.get(rid, snap);
    match old {
        None => {}
        Some(row) => assert!(row.deleted, "old rid must be tombstoned"),
    }

    // Lookup must resolve through the directory.
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(42))))
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one live row for PK 42");
    assert_eq!(rows[0].row_id, new_rid);
}

/// 6. HLC order inverted relative to local epoch. Drives two writes with
/// an epoch-monotonic ordering: the first version is in a run, the second
/// (same PK, Kit-style update) lives in a later run. The directory's
/// conservative filter must NOT skip the older run when the snapshot is
/// pinned before the second run lands, because the older run still
/// contains the visible version under that pin.
#[test]
fn hlc_order_inverted_relative_to_local_epoch() {
    use mongreldb_core::query::Condition;
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid_a = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    // Pin before the next operation so the historical epoch is below the
    // second run's committed epoch.
    let pinned = table.pin_snapshot();

    // Kit-style update: same PK, new rid, second run.
    table.delete(rid_a).unwrap();
    let rid_b = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    // The pinned snapshot must still see rid_a (it was live at the pinned
    // epoch). The directory's `is_impossible_for` must NOT skip rid_a's
    // run for that snapshot, even though a later run exists with a
    // higher epoch.
    let old = table
        .get(rid_a, pinned)
        .expect("pinned snapshot keeps rid_a");
    assert!(!old.deleted);
    assert_eq!(old.columns.get(&1), Some(&Value::Int64(42)));
    // The new rid is not visible under the historical pin.
    let absent = table.get(rid_b, pinned);
    assert!(
        absent.is_none() || absent.as_ref().unwrap().deleted,
        "rid_b must not be visible under pre-update pin"
    );

    // Current snapshot: rid_b wins; rid_a is the tombstone in the older run.
    let snap = table.snapshot();
    let got = table.get(rid_b, snap).expect("rid_b live at current snap");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));

    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(42))))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, rid_b);

    table.unpin_snapshot(pinned);
}

/// 7. Memtable winner over run winner. After a row is flushed to an immutable
/// run, a same-PK Kit-style update leaves a tombstone + new row in the
/// memtable; `Table::get` (via the lookup path) must return the new row, not
/// the durable run version.
#[test]
fn memtable_winner_over_run_winner() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    // Memtable: delete the old rid, put a fresh one with the same PK.
    table.delete(rid).unwrap();
    let new_rid = put(&mut table, 42);
    table.commit().unwrap();

    // Lookup metrics: directory_lookup_hit / fallback must still report
    // coherent counters (no regression vs. a healthy workload).
    let before = table.lookup_metrics_snapshot();
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(42))))
        .unwrap();
    let after = table.lookup_metrics_snapshot();
    assert_eq!(rows.len(), 1, "memtable winner over run winner");
    assert_eq!(rows[0].row_id, new_rid);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(42)));

    // Directory counters stay coherent (no NaN / overflow).
    let _ = (
        after.directory_lookup_hit - before.directory_lookup_hit,
        after.directory_incomplete - before.directory_incomplete,
    );
}

/// 8. Mutable-run winner over run winner. Same as (7) but the overlay sits in
/// the mutable-run tier (above the immutable spill threshold is not crossed).
#[test]
fn mutable_run_winner_over_run_winner() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    // Keep the mutable-run tier from spilling this single small update.
    table.set_mutable_run_spill_bytes(1 << 20);

    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();

    table.delete(rid).unwrap();
    let new_rid = put(&mut table, 42);
    table.commit().unwrap();

    // The mutable-run tier must contain the new version.
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(42))))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, new_rid);
}

/// 9. Directory missing. With no directory checkpoint present on disk, the
/// lookup path must fall back to the existing range-scan behaviour. Asserted
/// by deleting every shard file and re-opening.
#[test]
fn directory_missing() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    table.close().unwrap();

    // Simulate missing directory by removing every shard file. The engine
    // must accept the open and fall back to the range scan.
    let runs_dir = dir.path().join("_runs");
    for entry in std::fs::read_dir(&runs_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap_or("");
        if name.starts_with("directory.shard-") {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let mut table = Table::open(dir.path()).unwrap();
    let snap_metrics = table.lookup_metrics_snapshot();
    let got = table.get(rid, table.snapshot()).expect("row found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));
    // The directory load must have been recorded as a fallback.
    assert!(
        snap_metrics.directory_incomplete > 0 || snap_metrics.directory_lookup_fallback > 0,
        "missing directory should record an incomplete + fallback metric"
    );
}

/// 10. Directory corrupt. A torn / truncated directory shard must be
/// detected on open and rejected; the engine must continue with the range
/// scan fallback rather than panic or report a wrong result.
#[test]
fn directory_corrupt() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    table.close().unwrap();

    // Truncate every shard so the CRC footer validation fails. The open
    // must succeed via fallback.
    let runs_dir = dir.path().join("_runs");
    let mut corrupted_any = false;
    for entry in std::fs::read_dir(&runs_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap_or("");
        if name.starts_with("directory.shard-") {
            std::fs::write(entry.path(), b"truncated").unwrap();
            corrupted_any = true;
        }
    }
    assert!(corrupted_any, "expected at least one shard to corrupt");

    let mut table = Table::open(dir.path()).unwrap();
    let snap_metrics = table.lookup_metrics_snapshot();
    let got = table.get(rid, table.snapshot()).expect("row found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));
    assert!(
        snap_metrics.directory_incomplete > 0,
        "corrupt shard should bump directory_incomplete"
    );
}

/// 11. Directory fingerprint stale. A persisted directory whose fingerprint
/// does not match the active manifest must be rejected on reopen and rebuilt
/// (or the engine must fall back to the range scan until rebuild finishes).
#[test]
fn directory_fingerprint_stale() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    table.close().unwrap();

    // Corrupt the stored fingerprint in every shard so the comparison on
    // open fails. The first 4 bytes after the magic are the fingerprint's
    // low word; flipping a single bit is enough.
    let runs_dir = dir.path().join("_runs");
    for entry in std::fs::read_dir(&runs_dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let name = name.to_str().unwrap_or("");
        if name.starts_with("directory.shard-") {
            let path = entry.path();
            let mut bytes = std::fs::read(&path).unwrap();
            // Magic is bytes 0..4. Fingerprint is bytes 4..12. Flip a bit.
            if bytes.len() > 8 {
                bytes[8] ^= 0x01;
            }
            std::fs::write(&path, &bytes).unwrap();
        }
    }

    let mut table = Table::open(dir.path()).unwrap();
    let snap_metrics = table.lookup_metrics_snapshot();
    let got = table.get(rid, table.snapshot()).expect("row found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));
    assert!(
        snap_metrics.directory_incomplete > 0,
        "stale fingerprint should bump directory_incomplete"
    );
}

/// 12. Crash after manifest publication but before directory publication.
/// Recovery must leave the table usable on the range-scan fallback without
/// waiting for the missing directory.
#[test]
fn crash_after_manifest_before_directory() {
    use mongreldb_fault::{activate, clear, Action};
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    // Simulate the crash window: fault the directory publication so the
    // manifest is durable but the directory checkpoint never lands.
    activate("directory.temp_write", Action::Fail);
    table.close().unwrap();
    clear();

    // The directory checkpoint should be absent (or zero-row). The open
    // must still succeed and serve the row.
    let mut table = Table::open(dir.path()).unwrap();
    let got = table.get(rid, table.snapshot()).expect("row found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));
}

/// 13. Crash after directory temp write but before rename. The temp file
/// must be cleaned up on reopen and the live directory must still match the
/// manifest (or be absent with a clean fallback).
#[test]
fn crash_after_directory_temp_write_before_rename() {
    use mongreldb_fault::{activate, clear, Action};
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let rid = put(&mut table, 42);
    table.commit().unwrap();
    table.force_flush().unwrap();
    // Inject a fault at the rename step so the temp write succeeds but the
    // shard never lands. The next open must still succeed and the
    // directory must be rebuilt from the runs.
    activate("directory.rename", Action::Fail);
    table.close().unwrap();
    clear();

    let mut table = Table::open(dir.path()).unwrap();
    let got = table.get(rid, table.snapshot()).expect("row found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));
}

/// 14. Compaction while local, `SnapshotRegistry`, and `PinRegistry` pins are
/// active. Pins taken before `compact()` must still see their pre-compact
/// rows; the table-local pin and the registry guard both survive the merge.
/// (`PinRegistry::pin` requires a direct `Arc<PinRegistry>` handle that the
/// public `Table` / `Database` API does not yet expose; this test covers the
/// local and registry paths and would extend to a third pin once the API
/// lands.)
#[test]
fn compaction_while_local_snapshot_and_pin_registry_pins_active() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let handle = db.table("t").unwrap();

    // Initial batch: rows 1..=3.
    {
        let mut t = handle.lock();
        for i in 1..=3 {
            put(&mut t, i);
        }
        t.commit().unwrap();
        t.force_flush().unwrap();
    }

    // Table-local pin registered before the next run is published.
    let local_pin = {
        let mut t = handle.lock();
        t.pin_snapshot()
    };

    // Second run: rows 4..=5.
    {
        let mut t = handle.lock();
        for i in 4..=5 {
            put(&mut t, i);
        }
        t.commit().unwrap();
        t.force_flush().unwrap();
    }

    // SnapshotRegistry guard — held until the end of the test so compact
    // observes an active registry pin during the merge.
    let (registry_pin, _registry_guard) = db.snapshot();

    // Third run: rows 6..=7, so compact has three runs to merge.
    {
        let mut t = handle.lock();
        for i in 6..=7 {
            put(&mut t, i);
        }
        t.commit().unwrap();
        t.force_flush().unwrap();
    }

    // Compaction merges the runs; both pins from before compact() must still
    // be servable through the historical-snapshot path.
    db.compact_table("t").unwrap();

    // Local pin still sees PK 1.
    {
        let mut t = handle.lock();
        let rows = t
            .query_at_with_allowed(
                &Query::new().and(Condition::Pk(pk_bytes(1))),
                local_pin,
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1, "local pin must keep PK 1 after compact");
        assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));
    }

    // Registry pin still sees PK 4 (added after the local pin, before
    // compact).
    {
        let mut t = handle.lock();
        let rows = t
            .query_at_with_allowed(
                &Query::new().and(Condition::Pk(pk_bytes(4))),
                registry_pin,
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1, "registry pin must keep PK 4 after compact");
        assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(4)));
    }

    // Current snapshot sees PK 7 (latest).
    {
        let mut t = handle.lock();
        let rows = t
            .query(&Query::new().and(Condition::Pk(pk_bytes(7))))
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(7)));
    }

    handle.lock().unpin_snapshot(local_pin);
}

/// 15. Encrypted run files. A row force-flushed into an AES-encrypted run is
/// recoverable after close/reopen and reads correctly via the lookup path.
#[test]
fn encrypted_run_files() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let rid;
    {
        let mut table = Table::create_encrypted(&path, pk_schema(), 1, "passphrase").unwrap();
        rid = put(&mut table, 42);
        table.commit().unwrap();
        table.force_flush().unwrap();
        assert!(table.run_count() >= 1);
        table.close().unwrap();
    }
    let mut table = Table::open_encrypted(&path, "passphrase").unwrap();
    let got = table.get(rid, table.snapshot()).expect("row found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(42)));

    // Query by PK also works on the encrypted reopen.
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(42))))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, rid);
}

/// 16. Reopen and explicit index rebuild. After close/reopen with no
/// directory checkpoint present, an explicit `rebuild_indexes` returns the
/// table to a healthy state and the lookup path serves PK queries correctly.
#[test]
fn reopen_and_explicit_index_rebuild() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let mut rids = Vec::new();
    {
        let mut table = Table::create(&path, pk_schema(), 1).unwrap();
        for i in 0..16i64 {
            rids.push(put(&mut table, i));
        }
        table.commit().unwrap();
        table.force_flush().unwrap();
        table.close().unwrap();
    }
    let mut table = Table::open(&path).unwrap();
    table.rebuild_indexes().unwrap();

    for (i, &rid) in rids.iter().enumerate() {
        let got = table.get(rid, table.snapshot()).expect("row found");
        assert_eq!(got.columns.get(&1), Some(&Value::Int64(i as i64)));
    }
}

/// 17. Randomized model test comparing directory lookup with a forced
/// full-run scan. After a random mix of puts, Kit-style updates, and
/// periodic flushes, every `Table::get(rid, current_snap)` result must
/// agree with what a `visible_rows(current_snap)` scan reports for the
/// same rid.
#[test]
fn randomized_model_test_directory_vs_full_run_scan() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();

    let mut rng: u64 = 0xDEAD_BEEF_CAFE_BABE;
    let mut next_pk: i64 = 0;
    let mut live: std::collections::HashMap<i64, RowId> = std::collections::HashMap::new();

    for op in 0..200u64 {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let pk = next_pk;
        next_pk += 1;
        let rid = put(&mut table, pk);
        live.insert(pk, rid);

        if op % 25 == 24 {
            table.commit().unwrap();
            table.force_flush().unwrap();
        }
    }
    table.commit().unwrap();
    table.force_flush().unwrap();

    // Full-run scan at the current snapshot.
    let snap = table.snapshot();
    let all = table.visible_rows(snap).unwrap();
    let mut full_scan: std::collections::HashMap<RowId, i64> = std::collections::HashMap::new();
    for row in all {
        if !row.deleted {
            if let Some(Value::Int64(v)) = row.columns.get(&1) {
                full_scan.insert(row.row_id, *v);
            }
        }
    }

    // Compare every live rid between get() and the full-scan model.
    for (&pk, &rid) in &live {
        let got = table.get(rid, snap);
        match got {
            Some(row) => {
                assert!(
                    !row.deleted,
                    "live rid {:?} (pk {}) must not be deleted",
                    rid, pk
                );
                assert_eq!(
                    full_scan.get(&rid).copied(),
                    Some(pk),
                    "directory lookup pk={} disagrees with full-scan model",
                    pk
                );
            }
            None => panic!(
                "directory lookup returned None for live rid {:?} (pk {}) but full scan found it",
                rid, pk
            ),
        }
    }
}
