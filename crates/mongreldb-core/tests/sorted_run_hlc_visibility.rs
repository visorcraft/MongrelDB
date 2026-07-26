//! Sorted-run full-Snapshot HLC visibility — Issue 1 regression tests.
//!
//! The run materializer historically routed point reads and scans through
//! `RunReader::get_version(row_id, snapshot.epoch)` (and friends), pre-filtering
//! candidates with `epoch <= snapshot.epoch`. That logic hides an
//! HLC-visible winner whose local epoch exceeds the snapshot's epoch but whose
//! HLC stamp is within the snapshot's HLC watermark.
//!
//! The fix introduces `get_version_at` / `get_version_column_at` /
//! `get_version_visibility_at` / `visible_versions_at` /
//! `visible_positions_with_rids_at` / `visible_indices_at` /
//! `visible_indices_native_at` / `tombstoned_row_ids_at` /
//! `range_row_ids_visible_i64_at` / `range_row_ids_visible_f64_at` /
//! `null_row_ids_visible_at` / `into_visible_version_cursor_at`, all of which
//! take a full [`crate::epoch::Snapshot`] and honor
//! [`crate::epoch::Snapshot::observes_row`] under HLC authority. The legacy
//! epoch-only wrappers still exist and delegate to the full implementation.
//!
//! The fixture below is the canonical inversion case from issue §5.4:
//!
//! ```text
//! Version old:        epoch=9  HLC=300  value="old"
//! Version authoritative: epoch=50 HLC=400 value="authoritative"
//! Snapshot:                epoch=10 HLC=500
//! Expected:                "authoritative"
//! ```
//!
//! Without the fix, the run cursor rejects the epoch=50 row before HLC ever
//! enters the decision; with the fix, `observes_row` admits it (HLC=400 <= 500)
//! and `version_is_newer` prefers it over epoch=9.

use mongreldb_core::epoch::{Epoch, Snapshot};
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::sorted_run::{RunReader, RunWriter, SYS_COMMIT_TS};
use mongreldb_core::Database;
use mongreldb_types::hlc::HlcTimestamp;
use tempfile::tempdir;

fn hlc(physical_micros: u64) -> HlcTimestamp {
    HlcTimestamp {
        physical_micros,
        logical: 0,
        node_tiebreaker: 1,
    }
}

fn pk_schema() -> Schema {
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
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: Vec::new(),
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// Build the canonical mixed-authority fixture into a standalone run file:
/// `old` at epoch=9/HLC=300, `authoritative` at epoch=50/HLC=400.
fn write_inverted_run(dir: &std::path::Path) {
    let path = dir.join("inverted.sr");
    let stamp_old = hlc(300);
    let stamp_auth = hlc(400);
    let rows = vec![
        Row::new_with_hlc(mongreldb_core::RowId(7), Epoch(9), stamp_old)
            .with_column(1, Value::Int64(7))
            .with_column(2, Value::Bytes(b"old".to_vec())),
        Row::new_with_hlc(mongreldb_core::RowId(7), Epoch(50), stamp_auth)
            .with_column(1, Value::Int64(7))
            .with_column(2, Value::Bytes(b"authoritative".to_vec())),
    ];
    RunWriter::new(&pk_schema(), 1, Epoch(50), 0)
        .write(&path, &rows)
        .unwrap();
}

#[test]
fn get_version_at_picks_hlc_newer_winner_when_epoch_inverted() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    assert!(reader.has_column(SYS_COMMIT_TS));

    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let result = reader
        .get_version_at(mongreldb_core::RowId(7), snapshot)
        .expect("get_version_at")
        .expect("row 7 visible");
    assert_eq!(result.0, Epoch(50));
    let row = result.1;
    assert_eq!(
        row.columns.get(&2),
        Some(&Value::Bytes(b"authoritative".to_vec())),
        "HLC-newer must beat epoch-newer under HLC authority"
    );
    assert_eq!(row.commit_ts, Some(hlc(400)));
}

#[test]
fn get_version_at_legacy_epoch_only_falls_back_to_old() {
    // Dual-model: a legacy Snapshot::at snapshot must still surface stamped
    // rows by epoch (epoch compatibility, not HLC authority). With the fixture
    // epoch=9/HLC=300 wins under epoch<=10.
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let legacy = Snapshot::at(Epoch(10));
    let result = reader
        .get_version_at(mongreldb_core::RowId(7), legacy)
        .expect("legacy lookup")
        .expect("row 7 visible");
    assert_eq!(
        result.1.columns.get(&2),
        Some(&Value::Bytes(b"old".to_vec())),
        "legacy epoch-only snapshot must return the older epoch row"
    );
}

#[test]
fn get_version_at_hlc_snapshot_hides_later_hlc() {
    // Snapshot pinned at HLC=350 must hide both the epoch=9/HLC=300 (newer
    // than stamped `old`'s HLC, but old is 300 <= 350 here) — wait, both
    // candidates must be admitted by `observes_row`:
    //   old:        HLC=300 <= 350 → admitted
    //   auth:       HLC=400 <= 350 → hidden
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(350));
    let result = reader
        .get_version_at(mongreldb_core::RowId(7), snapshot)
        .expect("lookup")
        .expect("row 7 visible");
    assert_eq!(
        result.1.columns.get(&2),
        Some(&Value::Bytes(b"old".to_vec())),
        "HLC=400 must be hidden when snapshot HLC=350"
    );
}

#[test]
fn get_version_column_at_picks_hlc_newer() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let value = reader
        .get_version_column_at(mongreldb_core::RowId(7), snapshot, 2)
        .expect("column read")
        .expect("row visible");
    assert_eq!(
        value.2,
        Some(Value::Bytes(b"authoritative".to_vec())),
        "column-level HLC resolution must pick the winner"
    );
    assert_eq!(value.0, Epoch(50));
    assert!(!value.1, "winning row is not a tombstone");
}

#[test]
fn get_version_visibility_at_picks_hlc_newer() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let (epoch, deleted) = reader
        .get_version_visibility_at(mongreldb_core::RowId(7), snapshot)
        .expect("visibility read")
        .expect("row visible");
    assert_eq!(epoch, Epoch(50));
    assert!(!deleted);
}

#[test]
fn visible_versions_at_hlc_snapshot_orders_by_hlc() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let versions = reader
        .visible_versions_at(snapshot)
        .expect("visible_versions_at");
    assert_eq!(versions.len(), 1, "two rows collapse to one rid");
    assert_eq!(
        versions[0].columns.get(&2),
        Some(&Value::Bytes(b"authoritative".to_vec())),
    );
}

#[test]
fn visible_indices_at_hlc_snapshot_orders_by_hlc() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let idxs = reader
        .visible_indices_at(snapshot)
        .expect("visible_indices_at");
    assert_eq!(idxs.len(), 1);
}

#[test]
fn visible_positions_with_rids_at_hlc_snapshot_orders_by_hlc() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let mut reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let (positions, rids) = reader
        .visible_positions_with_rids_at(snapshot)
        .expect("visible_positions_with_rids_at");
    assert_eq!(positions.len(), 1);
    assert_eq!(rids, vec![7]);
}

#[test]
fn visible_version_cursor_at_picks_hlc_newer() {
    let dir = tempdir().unwrap();
    write_inverted_run(dir.path());
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let reader = RunReader::open(dir.path().join("inverted.sr"), pk_schema(), None).unwrap();
    let control = mongreldb_core::ExecutionControl::new(None);
    let mut cursor = reader
        .into_visible_version_cursor_at(snapshot)
        .expect("cursor");
    let first = cursor
        .next_visible_version(&control)
        .expect("next")
        .expect("row visible");
    assert_eq!(first.row_id, mongreldb_core::RowId(7));
    assert_eq!(first.committed_epoch, Epoch(50));
    assert_eq!(first.commit_ts, Some(hlc(400)));
    let row = cursor.materialize(first, &control).expect("materialize");
    assert_eq!(
        row.columns.get(&2),
        Some(&Value::Bytes(b"authoritative".to_vec())),
    );
    assert!(cursor
        .next_visible_version(&control)
        .expect("end")
        .is_none());
}

#[test]
fn hlc_visible_tombstone_suppresses_epoch_visible_live_row() {
    // Per rid: live row at epoch=5 (legacy, no HLC), tombstone at HLC=600
    // (epoch=10 stale). HLC-pinned snapshot HLC=400 still admits the live
    // tombstone-free row (epoch gate passes, no stamp on row 1). Then we
    // write a second rid whose tombstone lands in a later HLC inside the
    // window: row2 has live@epoch=5 and tombstone@HLC=350 (epoch=10). Under
    // HLC=400 the HLC-newer tombstone must suppress the live row.
    let dir = tempdir().unwrap();
    let path = dir.path().join("hlc-tombstone.sr");
    let stamp_tomb = hlc(350);
    let stamp_live = hlc(500);
    let mut tomb = Row::new_with_hlc(mongreldb_core::RowId(2), Epoch(10), stamp_tomb)
        .with_column(1, Value::Int64(2))
        .with_column(2, Value::Bytes(b"deleted".to_vec()));
    tomb.deleted = true;
    let rows = vec![
        // rid=1: live legacy row, no HLC; survives under HLC=400 pin
        // because legacy rules say: no stamp + HLC snap ⇒ fall back to epoch.
        Row::new(mongreldb_core::RowId(1), Epoch(5))
            .with_column(1, Value::Int64(1))
            .with_column(2, Value::Bytes(b"keeps".to_vec())),
        // rid=2: live@epoch=5 (legacy) AND tomb@epoch=10 HLC=350.
        // Under HLC=400 the tombstone (HLC=350) wins; live@epoch=5 has no
        // stamp so it's hidden by the HLC-newer tombstone in `version_is_newer`.
        Row::new(mongreldb_core::RowId(2), Epoch(5))
            .with_column(1, Value::Int64(2))
            .with_column(2, Value::Bytes(b"alive".to_vec())),
        tomb,
        // rid=3 stamped-only live, HLC=500 above the snapshot HLC=400
        // ⇒ hidden.
        Row::new_with_hlc(mongreldb_core::RowId(3), Epoch(1), stamp_live)
            .with_column(1, Value::Int64(3))
            .with_column(2, Value::Bytes(b"future".to_vec())),
    ];
    RunWriter::new(&pk_schema(), 2, Epoch(10), 0)
        .write(&path, &rows)
        .unwrap();

    let mut reader = RunReader::open(&path, pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(5), hlc(400));
    let versions = reader
        .visible_versions_at(snapshot)
        .expect("visible_versions_at");
    // The exact result set depends on `version_is_newer` for rid=2: the
    // tombstone (HLC=350, epoch=10) and live (no HLC, epoch=5). Mixed rule
    // prefers HLC when one side lacks HLC → tombstone wins.
    let rids: Vec<u64> = versions.iter().map(|r| r.row_id.0).collect();
    assert!(rids.contains(&1), "legacy live rid=1 must be visible");
    let rid2 = versions
        .iter()
        .find(|r| r.row_id.0 == 2)
        .expect("rid=2 must have a newest visible candidate");
    assert!(
        rid2.deleted,
        "HLC=350 tombstone must beat legacy-live@epoch=5 for rid=2"
    );
    assert!(!rids.contains(&3), "rid=3 HLC=500 > HLC=400 must be hidden");

    // `tombstoned_row_ids_at` reports the rid(s) whose newest visible
    // version is a tombstone. rid=2 must be reported.
    let tombs = reader
        .tombstoned_row_ids_at(snapshot)
        .expect("tombstoned_row_ids_at");
    assert!(
        tombs.contains(&2),
        "HLC-visible tombstone must be reported: {tombs:?}"
    );
    assert!(!tombs.contains(&1), "rid=1 is live legacy, not a tombstone");
}

#[test]
fn two_stamped_rows_ordered_by_hlc() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("two-stamped.sr");
    let early = hlc(100);
    let late = hlc(500);
    let rows = vec![
        Row::new_with_hlc(mongreldb_core::RowId(1), Epoch(1), early)
            .with_column(1, Value::Int64(1))
            .with_column(2, Value::Bytes(b"early".to_vec())),
        Row::new_with_hlc(mongreldb_core::RowId(1), Epoch(2), late)
            .with_column(1, Value::Int64(1))
            .with_column(2, Value::Bytes(b"late".to_vec())),
    ];
    RunWriter::new(&pk_schema(), 3, Epoch(2), 0)
        .write(&path, &rows)
        .unwrap();
    let mut reader = RunReader::open(&path, pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), late);
    let (_, row) = reader
        .get_version_at(mongreldb_core::RowId(1), snapshot)
        .expect("lookup")
        .expect("visible");
    assert_eq!(
        row.columns.get(&2),
        Some(&Value::Bytes(b"late".to_vec())),
        "HLC-newer must win"
    );
}

#[test]
fn stamped_vs_unstamped_falls_back_to_mixed_rule() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("mixed.sr");
    let stamped = hlc(100);
    let rows = vec![
        // stamped @ high epoch=10, low HLC=100
        Row::new_with_hlc(mongreldb_core::RowId(1), Epoch(10), stamped)
            .with_column(1, Value::Int64(1))
            .with_column(2, Value::Bytes(b"stamped".to_vec())),
        // unstamped @ lower epoch=3 — no SYS_COMMIT_TS cell
        Row::new(mongreldb_core::RowId(1), Epoch(3))
            .with_column(1, Value::Int64(1))
            .with_column(2, Value::Bytes(b"plain".to_vec())),
    ];
    RunWriter::new(&pk_schema(), 4, Epoch(10), 0)
        .write(&path, &rows)
        .unwrap();
    let mut reader = RunReader::open(&path, pk_schema(), None).unwrap();
    // HLC-pinned snapshot at HLC=200 is high enough to admit both; mixed
    // rule says epoch wins when one side lacks HLC. epoch=10 wins.
    let snapshot = Snapshot::at_hlc(Epoch(20), hlc(200));
    let (_, row) = reader
        .get_version_at(mongreldb_core::RowId(1), snapshot)
        .expect("lookup")
        .expect("visible");
    assert_eq!(
        row.columns.get(&2),
        Some(&Value::Bytes(b"stamped".to_vec())),
        "stamped row wins by epoch under mixed rule"
    );
}

#[test]
fn table_get_hlc_visibility_dense() {
    // Drive the same fixture through `Database` so the public Table API
    // (the "Table::get" required surface) honors HLC authority. We use
    // `Database::apply_staged_txn_writes` to land stamped rows directly,
    // flush to a run, and re-open a fresh handle so the hot tier no
    // longer shades the run state.
    use mongreldb_core::database::StagedTxnWrite;
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("t", pk_schema()).unwrap();
    let table_id = db.table_id("t").unwrap();

    fn encode_put(table_id: u64, row: &Row) -> Vec<u8> {
        bincode::serialize(&StagedTxnWrite::Put {
            table_id,
            rows: bincode::serialize(&vec![row.clone()]).expect("encode row"),
        })
        .expect("encode payload")
    }

    let old = Row::new_with_hlc(mongreldb_core::RowId(7), Epoch(9), hlc(300))
        .with_column(1, Value::Int64(7))
        .with_column(2, Value::Bytes(b"old".to_vec()));
    let auth = Row::new_with_hlc(mongreldb_core::RowId(7), Epoch(50), hlc(400))
        .with_column(1, Value::Int64(7))
        .with_column(2, Value::Bytes(b"authoritative".to_vec()));

    db.apply_staged_txn_writes(1, &[encode_put(table_id, &old)], hlc(300))
        .expect("first apply");
    db.apply_staged_txn_writes(2, &[encode_put(table_id, &auth)], hlc(400))
        .expect("second apply");

    // Flush memtable + mutable run to an immutable `.sr`, then re-open a
    // fresh table handle so the reads hit the run reader — proving the
    // run path (not just the in-memory overlay) honors HLC authority.
    {
        let table = db.table("t").unwrap();
        table.lock().force_flush().expect("flush");
    }
    let handle = db.table("t").unwrap();
    let table = handle.lock();
    // Pin a fresh at_hlc snapshot directly for the read — the run reader
    // must surface the HLC-newer winner.
    let snap = Snapshot::at_hlc(Epoch(10), hlc(500));
    let row = table
        .get(mongreldb_core::RowId(7), snap)
        .expect("Table::get must surface the HLC-newer winner");
    assert_eq!(
        row.columns.get(&2),
        Some(&Value::Bytes(b"authoritative".to_vec())),
        "Table::get must honor HLC authority across the run"
    );
}

#[test]
fn reopen_from_disk_preserves_hlc_visibility() {
    // The fixture flushed, then the run file is reopened via RunReader
    // directly (no Database involved). This is the "reopen from disk"
    // required surface.
    let dir = tempdir().unwrap();
    let path = dir.path().join("reopen.sr");
    let rows = vec![
        Row::new_with_hlc(mongreldb_core::RowId(7), Epoch(9), hlc(300))
            .with_column(1, Value::Int64(7))
            .with_column(2, Value::Bytes(b"old".to_vec())),
        Row::new_with_hlc(mongreldb_core::RowId(7), Epoch(50), hlc(400))
            .with_column(1, Value::Int64(7))
            .with_column(2, Value::Bytes(b"authoritative".to_vec())),
    ];
    RunWriter::new(&pk_schema(), 99, Epoch(50), 0)
        .write(&path, &rows)
        .unwrap();

    // Re-open from the same file path and assert HLC authority still wins.
    let mut reader = RunReader::open(&path, pk_schema(), None).unwrap();
    let snapshot = Snapshot::at_hlc(Epoch(10), hlc(500));
    let (_, row) = reader
        .get_version_at(mongreldb_core::RowId(7), snapshot)
        .expect("lookup")
        .expect("visible");
    assert_eq!(
        row.columns.get(&2),
        Some(&Value::Bytes(b"authoritative".to_vec())),
    );
}
