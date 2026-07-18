//! Page-index pruning: PageStat min/max are populated at encode time for both
//! the flush (`write`) and bulk-load (`write_native`) paths.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{read_column_dir, read_header, Table, Value};
use std::path::PathBuf;
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
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "f".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "v_bitmap".into(),
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

fn be_i64(b: &Vec<u8>) -> i64 {
    i64::from_be_bytes(b.as_slice().try_into().unwrap())
}
fn be_u64(b: &Vec<u8>) -> u64 {
    u64::from_be_bytes(b.as_slice().try_into().unwrap())
}

fn run_file(db: &Table) -> PathBuf {
    let dir = db.dir();
    let mut p = dir.join("_runs").join("r-1.sr");
    if !p.exists() {
        let mut best = 0u64;
        for e in std::fs::read_dir(dir.join("_runs")).unwrap() {
            let name = e.unwrap().file_name().into_string().unwrap();
            if let Some(n) = name
                .strip_prefix("r-")
                .and_then(|s| s.strip_suffix(".sr"))
                .and_then(|s| s.parse::<u64>().ok())
            {
                best = best.max(n);
            }
        }
        p = dir.join("_runs").join(format!("r-{best}.sr"));
    }
    p
}

#[test]
fn page_stats_populated_on_flush() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.set_mutable_run_spill_bytes(1); // spill so a run's page stats can be inspected
    for i in 0..50i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Int64(100 + i * 2)), // 100..198
            (3, Value::Float64(1.5 + i as f64)),
        ])
        .unwrap();
    }
    db.flush().unwrap();

    let path = run_file(&db);
    let header = read_header(&path).unwrap();
    let col_dir = read_column_dir(&path, &header).unwrap();

    let v_stat = col_dir
        .iter()
        .find(|c| c.column_id == 2)
        .unwrap()
        .page_stats[0]
        .clone();
    assert_eq!(v_stat.row_count, 50);
    assert_eq!(be_i64(v_stat.min.as_ref().unwrap()), 100);
    assert_eq!(be_i64(v_stat.max.as_ref().unwrap()), 198);

    let f_stat = col_dir
        .iter()
        .find(|c| c.column_id == 3)
        .unwrap()
        .page_stats[0]
        .clone();
    let fmin = f64::from_bits(be_u64(f_stat.min.as_ref().unwrap()));
    let fmax = f64::from_bits(be_u64(f_stat.max.as_ref().unwrap()));
    assert!((fmin - 1.5).abs() < 1e-9);
    assert!((fmax - 50.5).abs() < 1e-9);
}

#[test]
fn page_stats_populated_on_bulk_load() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load(
        (0..1000i64)
            .map(|i| {
                vec![
                    (1, Value::Int64(i)),
                    (2, Value::Int64(5000 - i)),
                    (3, Value::Float64(i as f64 * 2.0)),
                ]
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let path = run_file(&db);
    let header = read_header(&path).unwrap();
    let col_dir = read_column_dir(&path, &header).unwrap();
    let v_stat = col_dir
        .iter()
        .find(|c| c.column_id == 2)
        .unwrap()
        .page_stats[0]
        .clone();
    assert_eq!(v_stat.row_count, 1000);
    assert_eq!(be_i64(v_stat.min.as_ref().unwrap()), 4001);
    assert_eq!(be_i64(v_stat.max.as_ref().unwrap()), 5000);
}

#[test]
fn multi_page_range_skipping() {
    use mongreldb_core::query::{Condition, Query};
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    db.bulk_load(
        (0..200_000i64)
            .map(|i| {
                vec![
                    (1, Value::Int64(i)),
                    (2, Value::Int64(i)), // == row id, strictly increasing
                    (3, Value::Float64(i as f64)),
                ]
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();

    // 200k rows ⇒ the int column must span several 65 536-row pages.
    let path = run_file(&db);
    let header = read_header(&path).unwrap();
    let col_dir = read_column_dir(&path, &header).unwrap();
    let v_header = col_dir.iter().find(|c| c.column_id == 2).unwrap();
    assert!(
        v_header.page_count > 1,
        "expected multiple pages, got {}",
        v_header.page_count
    );

    // A tight middle range prunes to ~1 page; the answer must be exact (2 rows).
    let q = Query::new().and(Condition::Range {
        column_id: 2,
        lo: 100_000,
        hi: 100_001,
    });
    let rows = db.query(&q).unwrap();
    assert_eq!(rows.len(), 2);

    // And a float range on the same multi-page column.
    let qf = Query::new().and(Condition::RangeF64 {
        column_id: 3,
        lo: 50_000.0,
        lo_inclusive: true,
        hi: 50_002.0,
        hi_inclusive: false,
    });
    let rows_f = db.query(&qf).unwrap();
    assert_eq!(rows_f.len(), 2); // 50000.0, 50001.0
}
