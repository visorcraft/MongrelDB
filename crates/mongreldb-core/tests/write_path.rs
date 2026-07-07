//! End-to-end smoke test for the Phase-0 write path: the group-commit WAL, the
//! skip-list memtable, the HOT primary index, a memtable→sorted-run flush of
//! the container format, and the AI-native indexes intersecting in the shared
//! row-id space.

use mongreldb_core::{
    index::{AnnIndex, BitmapIndex, FmIndex, HotIndex},
    schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId},
    sorted_run::{write_run, ColumnPayload, RunSpec},
    Encoding, Epoch, RowId, Snapshot, Value,
};

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
            },
            ColumnDef {
                id: 2,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "by_label".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

#[test]
fn write_path_prototype() {
    // --- 1. Group-commit write path (the sub-ms path) ----------------------
    use mongreldb_core::Table;
    let dir = tempfile::tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();

    let mut row_ids = Vec::new();
    for (i, label) in ["red", "blue", "red", "green"].iter().enumerate() {
        let r = db
            .put(vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Bytes(label.as_bytes().to_vec())),
            ])
            .unwrap();
        row_ids.push(r);
    }
    let committed = db.commit().unwrap();
    let snap = Snapshot::at(committed);
    for &r in &row_ids {
        assert!(db.get(r, snap).is_some());
    }

    // --- 2. Flush the memtable into a sorted run (container round-trip) ----
    let rows = db.drain_memtable_sorted();
    let labels: Vec<&[u8]> = rows
        .iter()
        .map(|r| match r.columns.get(&2) {
            Some(Value::Bytes(b)) => b.as_slice(),
            _ => b"",
        })
        .collect();
    let label_pages: Vec<Vec<u8>> = labels.into_iter().map(|l| l.to_vec()).collect();
    let run_dir = dir.path().join("_runs");
    std::fs::create_dir_all(&run_dir).unwrap();
    let header = write_run(
        run_dir.join("r-1.sr"),
        &RunSpec {
            run_id: 1,
            schema_id: 1,
            epoch_created: committed.0,
            level: 0,
            flags: 0,
            sort_key_column_id: 0xFFFF,
            row_count: rows.len() as u64,
            min_row_id: 0,
            max_row_id: row_ids.last().unwrap().0,
            columns: &[ColumnPayload {
                column_id: 2,
                type_id_tag: 12,
                encoding: Encoding::Plain,
                pages: label_pages,
                page_stats: Vec::new(),
            }],
        },
    )
    .unwrap();
    let read_back = mongreldb_core::read_header(run_dir.join("r-1.sr")).unwrap();
    assert_eq!(read_back.content_hash, header.content_hash);

    // --- 3. AI-native indexes intersect in the shared row-id space ---------
    // Primary-key HOT: label "red" first row is row 0.
    let mut hot = HotIndex::new();
    hot.insert(Value::Int64(0).encode_key(), RowId(0));
    assert_eq!(hot.get(&Value::Int64(0).encode_key()), Some(RowId(0)));

    // Bitmap secondary: label -> row-id set.
    let mut bmp = BitmapIndex::new();
    for (i, label) in ["red", "blue", "red", "green"].iter().enumerate() {
        bmp.insert(label.as_bytes().to_vec(), RowId(i as u64));
    }
    let reds = bmp.get(b"red");

    // FM substring index.
    let mut fm = FmIndex::new();
    fm.insert(b"the red fox".to_vec(), RowId(0));
    fm.insert(b"blue sky".to_vec(), RowId(1));
    let fox_hits = fm.locate(b"fox");

    // ANN: 8-dim toy embeddings.
    let mut ann = AnnIndex::new(8);
    ann.insert(&[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0], RowId(0));
    ann.insert(&[-1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, 1.0], RowId(1));
    let top = ann.search(&[1.0, 1.0, 1.0, 1.0, -1.0, -1.0, -1.0, -1.0], 1);
    assert_eq!(top[0].0, RowId(0));

    // The shared row-id space lets an agent compose these freely.
    let composed: Vec<u32> = reds
        .iter()
        .filter(|rid| fox_hits.iter().any(|h| h.0 == *rid as u64))
        .collect();
    let _ = composed; // smoke test only
    let _ = (read_back, committed);
    let _ = Epoch::default();
}
