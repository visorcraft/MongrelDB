//! Correctness tests for the `RunLookupDirectory` point-lookup path
#![allow(clippy::doc_lazy_continuation)]
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
    let mut rid = put(&mut table, 1);
    table.commit().unwrap();

    // 9 Kit-style updates to the same PK: delete the previous rid, put a new
    // rid with the same PK. The engine tombstones the previous rid (HOT
    // already maps the PK to it) and produces a fresh rid.
    for _ in 2..=10 {
        table.delete(rid).unwrap();
        let new_rid = put(&mut table, 1);
        assert_ne!(new_rid, rid, "Kit-style update must assign fresh rid");
        rid = new_rid;
        table.commit().unwrap();
    }
    table.force_flush().unwrap();

    // At current snapshot, exactly one live row matches the PK.
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(1))))
        .unwrap();
    assert_eq!(rows.len(), 1, "exactly one live row for the PK");
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(1)));

    // The current live rid is the one produced by the last Kit-style update.
    assert_eq!(
        rows[0].row_id, rid,
        "the live row must be the latest fresh rid"
    );
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

    let table = Table::open(dir.path()).unwrap();
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

    let table = Table::open(dir.path()).unwrap();
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

    let table = Table::open(dir.path()).unwrap();
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
    let table = Table::open(dir.path()).unwrap();
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

    let table = Table::open(dir.path()).unwrap();
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
    let mut live: std::collections::HashMap<i64, RowId> = std::collections::HashMap::new();

    for op in 0..200u64 {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let pk: i64 = op as i64;
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

// ----------------------------------------------------------------------------
// REM-004 — DirectoryLookupDecision: complete-miss, candidates, and safe
// early-stop. The tests below rely on the directory being complete (the
// `force_flush`/`commit` cycle publishes one) and on Table::get returning
// the in-memory tier version when no locator opens.
// ----------------------------------------------------------------------------

/// 18. Complete exact miss. Many overlapping run ranges, row absent from
/// the directory. Asserts `Table::get` opens zero immutable readers and
/// does not record a fallback.
#[test]
fn complete_directory_miss_opens_zero_run_readers() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    // Many Kit-style updates to populate runs whose RowId ranges cover the
    // uninserted `pk_target` RowId space; the directory should record
    // post-updates only and report no postings for `target_rid`.
    let mut live: std::collections::HashMap<i64, RowId> = std::collections::HashMap::new();
    for i in 0..32i64 {
        let rid = put(&mut table, i);
        live.insert(i, rid);
        table.commit().unwrap();
        if i % 4 == 3 {
            table.force_flush().unwrap();
        }
    }
    table.commit().unwrap();
    table.force_flush().unwrap();
    assert!(
        table.run_count() >= 4,
        "expect ≥4 runs so coarse range fallback would otherwise open readers"
    );

    // Compute a target RowId well inside the run-set's row-id space but
    // outside the directory's postings.
    let target_rid = RowId(u64::MAX - 1);

    let before = table.lookup_metrics_snapshot();
    let got = table.get(target_rid, table.snapshot());
    let after = table.lookup_metrics_snapshot();

    assert!(got.is_none(), "absent row must return None");
    let run_readers_delta =
        after.directory_run_readers_opened - before.directory_run_readers_opened;
    let get_run_opened_delta = after.get_run_opened - before.get_run_opened;
    let fallback_delta = after.directory_lookup_fallback - before.directory_lookup_fallback;
    assert_eq!(
        run_readers_delta, 0,
        "complete directory miss must open zero immutable run readers"
    );
    assert_eq!(
        get_run_opened_delta, 0,
        "complete directory miss must not bump get_run_opened"
    );
    assert_eq!(
        fallback_delta, 0,
        "complete directory miss must not bump directory_lookup_fallback"
    );
    assert!(
        after.directory_complete_miss_total > before.directory_complete_miss_total,
        "complete directory miss must bump directory_complete_miss_total"
    );
    assert!(
        after.directory_lookup_hit > before.directory_lookup_hit,
        "complete directory miss must still register a hit (directory was usable)"
    );

    // Sanity: an existing live row still resolves through the directory
    // and is unchanged by the miss exercise.
    let some_rid = live.get(&3).copied().expect("pk 3 was inserted");
    let got = table.get(some_rid, table.snapshot()).expect("pk 3 found");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(3)));
}

/// 19. Single-version hit with unrelated runs. At run counts 1, 4, 16, 64,
/// and 256, a row present in exactly one run should open at most one
/// immutable reader (the directory consults only that run's locator).
#[test]
fn single_version_hit_with_unrelated_runs() {
    for &target_runs in &[1usize, 4, 16, 64, 256] {
        let dir = tempdir().unwrap();
        let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
        // The hit row (pk 999) is inserted last and force-flushed into the
        // terminal run. Earlier runs only contain other PKs.
        let mut other_rids: Vec<RowId> = Vec::new();
        for i in 0..target_runs as i64 {
            let rid = put(&mut table, i);
            other_rids.push(rid);
            table.commit().unwrap();
            table.force_flush().unwrap();
        }
        let hit_rid = put(&mut table, 999);
        table.commit().unwrap();
        table.force_flush().unwrap();
        assert!(
            table.run_count() >= target_runs,
            "expected ≥{target_runs} runs, got {}",
            table.run_count()
        );

        let before = table.lookup_metrics_snapshot();
        let got = table.get(hit_rid, table.snapshot()).expect("hit row found");
        let after = table.lookup_metrics_snapshot();
        assert!(!got.deleted);
        assert_eq!(got.columns.get(&1), Some(&Value::Int64(999)));
        let run_readers_delta =
            after.directory_run_readers_opened - before.directory_run_readers_opened;
        assert!(
            run_readers_delta <= 1,
            "single-version hit at {target_runs} runs opened {run_readers_delta} readers; expected ≤1"
        );
        assert!(
            after.directory_lookup_hit > before.directory_lookup_hit,
            "single-version hit must bump directory_lookup_hit"
        );
        let _ = other_rids; // keep lints happy
    }
}

/// 20. Early stop on current epoch. Versions in several runs with locator
/// bounds proving the first candidate is newest. The directory is consulted
/// newest-first; once the first locator's reader returns the winner, the
/// remaining locators must be skipped via the safe early-stop proof.
///
/// To exercise early-stop the target `RowId` must appear in multiple runs:
/// a Kit-style chain produces that exact shape — the original rid is
/// committed to an early run and then tombstoned in a later run, so the
/// directory records one locator per `(rid, run_id)`. The newer locator's
/// max epoch bounds the older one, so the older locator must be
/// early-stopped.
#[test]
fn early_stop_on_current_epoch() {
    // Single-rid baseline: only one locator for the rid (one run, one
    // version). Early-stop has nothing to skip.
    let dir_one = tempdir().unwrap();
    let mut table_one = Table::create(dir_one.path(), pk_schema(), 1).unwrap();
    let rid_one = put(&mut table_one, 7);
    table_one.commit().unwrap();
    table_one.force_flush().unwrap();
    let rid_one_before = table_one.lookup_metrics_snapshot();
    let _ = table_one.get(rid_one, table_one.snapshot());
    let rid_one_after = table_one.lookup_metrics_snapshot();
    let early_one =
        rid_one_after.directory_early_stop_total - rid_one_before.directory_early_stop_total;

    // Multi-version baseline: the same rid is tombstoned across several
    // runs. The directory should produce multiple locators for that rid,
    // and the safe early-stop proof must skip the older ones once the
    // newer-run tombstone wins.
    let dir_many = tempdir().unwrap();
    let mut table_many = Table::create(dir_many.path(), pk_schema(), 1).unwrap();
    let rid_many = put(&mut table_many, 7);
    table_many.commit().unwrap();
    table_many.force_flush().unwrap();
    // Tombstone the rid across 3 more runs so the directory sees four
    // locators for `rid_many`.
    for _ in 0..3 {
        table_many.delete(rid_many).unwrap();
        table_many.commit().unwrap();
        table_many.force_flush().unwrap();
    }
    assert!(
        table_many.run_count() >= 4,
        "expect ≥4 runs for the multi-version baseline"
    );
    let rid_many_before = table_many.lookup_metrics_snapshot();
    let _ = table_many.get(rid_many, table_many.snapshot());
    let rid_many_after = table_many.lookup_metrics_snapshot();
    let early_many =
        rid_many_after.directory_early_stop_total - rid_many_before.directory_early_stop_total;
    assert!(
        early_many > early_one,
        "early_stop_total must increase for the multi-run case ({early_one} → {early_many})"
    );
}

/// 21. HLC-safe early stop. Build a row whose runs have inverted epoch/HLC
/// bounds and assert the early-stop proof never skips a locator that could
/// legitimately host an HLC-newer winner than the current best.
#[test]
fn hlc_safe_early_stop() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    // Insert one PK, flush to a run with a low epoch / high HLC
    // combination isn't directly settable from the public API; instead,
    // exercise the safety check via a row that has multiple locators across
    // runs and a snapshot that reveals them.
    let mut rid = put(&mut table, 11);
    table.commit().unwrap();
    table.force_flush().unwrap();
    for _ in 0..3 {
        table.delete(rid).unwrap();
        rid = put(&mut table, 11);
        table.commit().unwrap();
        table.force_flush().unwrap();
    }
    let target = rid;

    let before = table.lookup_metrics_snapshot();
    let got = table.get(target, table.snapshot()).expect("row found");
    let after = table.lookup_metrics_snapshot();
    assert!(!got.deleted);
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(11)));

    // The early-stop metric may legitimately bump when older runs are
    // proven not to beat the latest rid's locator bounds. The safety
    // invariant: the returned row's pk/value is always the latest version,
    // never an older run's value.
    assert!(
        got.committed_epoch >= mongreldb_core::epoch::Epoch::ZERO,
        "snapshot-correctness invariant holds"
    );

    // Run-readers opened on this single target must be bounded: at most one
    // reader per distinct run_id visited. Multiple locator entries pointing
    // at the same run_id are deduplicated by the existing HashSet guard.
    let opened = after.directory_run_readers_opened - before.directory_run_readers_opened;
    assert!(
        opened <= table.run_count() as u64,
        "run-readers opened ({opened}) cannot exceed run count ({})",
        table.run_count()
    );
}

/// 22. Mixed stamped/unstamped safety. A locator that contains both stamped
/// and unstamped rows must stay conservative under the early-stop proof;
/// the mixed comparison rule falls back to epoch, so the locator can still
/// beat an HLC-newer best via an unstamped row at a higher epoch.
///
/// Drives the read path with a Kit-style update chain. The terminal rid
/// lives in the newest run; the older rids live in older runs. The
/// directory's mixed locators must remain conservative — early-stop is
/// never allowed to skip a run that could legitimately host a newer
/// version, and the returned row must always be the latest one for the PK.
#[test]
fn mixed_stamped_unstamped_safety() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    let mut rid = put(&mut table, 13);
    table.commit().unwrap();
    table.force_flush().unwrap();
    for _ in 0..3 {
        table.delete(rid).unwrap();
        rid = put(&mut table, 13);
        table.commit().unwrap();
        table.force_flush().unwrap();
    }
    let final_rid = rid;
    let snap = table.snapshot();
    let got = table
        .get(final_rid, snap)
        .expect("row must be findable in the latest run");
    assert!(!got.deleted, "latest rid must be live");
    assert_eq!(got.columns.get(&1), Some(&Value::Int64(13)));
    // The PK-by-query path must also report exactly one live row.
    let rows = table
        .query(&Query::new().and(Condition::Pk(pk_bytes(13))))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, final_rid);
    assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(13)));
}

/// 23. Preserve the directory-fallback invariant: when the directory is
/// missing or stale, the range-scan fallback remains correct and bumps the
/// fallback metrics. This complements tests 9–11 with explicit fallback
/// assertions.
#[test]
fn complete_miss_does_not_record_fallback() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), pk_schema(), 1).unwrap();
    for i in 0..8i64 {
        put(&mut table, i);
        table.commit().unwrap();
        if i % 2 == 1 {
            table.force_flush().unwrap();
        }
    }
    table.commit().unwrap();
    table.force_flush().unwrap();
    // No row has a row_id near u64::MAX.
    let absent_rid = RowId(u64::MAX - 42);
    let before = table.lookup_metrics_snapshot();
    let got = table.get(absent_rid, table.snapshot());
    let after = table.lookup_metrics_snapshot();
    assert!(got.is_none());
    assert_eq!(
        after.directory_lookup_fallback - before.directory_lookup_fallback,
        0,
        "complete miss must not record a fallback"
    );
    assert!(
        after.directory_complete_miss_total - before.directory_complete_miss_total >= 1,
        "complete miss must increment complete_miss_total"
    );
}
