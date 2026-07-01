//! Apples-to-apples MongrelDB benchmark on the compare.py schema
//! (id int64, value float64, name utf8). Runs each measurement multiple times
//! and reports the best (min) to filter sandbox contention.

use mongreldb_core::{columnar::NativeColumn, schema::*, Table};
use std::time::Instant;
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
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: Vec::new(),
        colocation: vec![], constraints: Default::default(),
    }
}

fn full_validity(n: usize) -> Vec<u8> {
    vec![0xFF; n.div_ceil(8)]
}

fn build_cols(n: usize) -> Vec<(u16, NativeColumn)> {
    let id_col = NativeColumn::int64_sequence(0, n);
    let value_col = NativeColumn::Float64 {
        data: (0..n).map(|i| i as f64).collect(),
        validity: full_validity(n),
    };
    let mut offsets = vec![0u32];
    let mut values = Vec::new();
    for i in 0..n {
        values.extend_from_slice(format!("name_{i}").as_bytes());
        offsets.push(values.len() as u32);
    }
    let name_col = NativeColumn::Bytes {
        offsets,
        values,
        validity: full_validity(n),
    };
    vec![(1u16, id_col), (2, value_col), (3, name_col)]
}

fn main() {
    println!("MongrelDB (Arrow-native), schema: id int64, value float64, name utf8");
    println!("Each metric: best of 3-5 runs.\n");
    println!(
        "{:>8} {:>12} {:>12} {:>10} {:>10} {:>10}",
        "rows", "ingest_Mr/s", "scan_Mr/s", "scan_ms", "count_us", "bytes/row"
    );

    for &n in &[100usize, 10_000, 100_000, 1_000_000] {
        // --- Ingest: best of 3 ---
        let mut best_ingest = f64::MAX;
        let mut bpr = 0.0_f64;
        for _ in 0..3 {
            let dir = tempdir().unwrap();
            let mut db = Table::create(dir.path(), schema(), 1).unwrap();
            let cols = build_cols(n);
            let t = Instant::now();
            db.bulk_load_columns(cols).unwrap();
            let s = t.elapsed().as_secs_f64();
            if s < best_ingest {
                best_ingest = s;
            }
            // bytes/row from the first run
            let mut bytes = 0u64;
            for e in std::fs::read_dir(dir.path().join("_runs")).unwrap() {
                bytes += std::fs::metadata(e.unwrap().path()).unwrap().len();
            }
            bpr = bytes as f64 / n as f64;
        }
        let ingest_mrs = n as f64 / best_ingest / 1e6;

        // --- Load once for scan + update ---
        let dir = tempdir().unwrap();
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.bulk_load_columns(build_cols(n)).unwrap();
        let snap = db.snapshot();

        // --- Scan: best of 5 ---
        let _ = db.visible_columns_native(snap, None).unwrap(); // warm
        let mut best_scan = f64::MAX;
        for _ in 0..5 {
            let t = Instant::now();
            let cols = db.visible_columns_native(snap, None).unwrap();
            let s = t.elapsed().as_secs_f64();
            if s < best_scan {
                best_scan = s;
            }
            let _ = cols;
        }
        let scan_mrs = n as f64 / best_scan / 1e6;
        let scan_ms = best_scan * 1000.0;

        // --- Count (O(1)) ---
        let t = Instant::now();
        let _ = db.count();
        let count_us = t.elapsed().as_micros();

        // --- Update: best of 5 (table-size independent) ---
        // let mut best_upd = f64::MAX;
        // for _ in 0..5 {
        //     let t = Instant::now();
        //     db.put(vec![(1, Value::Int64(0))]).unwrap();
        //     db.commit().unwrap();
        //     let s = t.elapsed().as_secs_f64();
        //     if s < best_upd { best_upd = s; }
        // }
        // let upd_us = best_upd * 1e6;

        println!(
            "{:>8} {:>12.2} {:>12.2} {:>10.2} {:>10} {:>10.2}",
            n, ingest_mrs, scan_mrs, scan_ms, count_us, bpr
        );
    }
}
