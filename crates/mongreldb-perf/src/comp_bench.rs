//! MongrelDB equivalent of the IcefallDB/DuckDB benchmark queries.
//! Measures p50/p95/min for warm (cached) and cold runs on 1M rows.

use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::schema::*;
use mongreldb_core::Table;
use mongreldb_query::MongrelSession;
use std::time::{Duration, Instant};
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
                name: "category".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "status".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 4,
                name: "amount".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 5,
                name: "ts".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 6,
                name: "user_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 7,
                name: "region".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![
            IndexDef {
                name: "cat_bm".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
            predicate: None,
            },
            IndexDef {
                name: "status_bm".into(),
                column_id: 3,
                kind: IndexKind::Bitmap,
            predicate: None,
            },
            IndexDef {
                name: "region_bm".into(),
                column_id: 7,
                kind: IndexKind::Bitmap,
            predicate: None,
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn users_schema() -> Schema {
    Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "uid".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "region".into(),
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

fn mk_bytes_col(vals: &[Vec<u8>]) -> NativeColumn {
    let offsets: Vec<u32> = std::iter::once(0u32)
        .chain(vals.iter().scan(0u32, |acc, v| {
            *acc += v.len() as u32;
            Some(*acc)
        }))
        .collect();
    let values: Vec<u8> = vals.iter().flat_map(|v| v.iter().copied()).collect();
    NativeColumn::Bytes {
        offsets,
        values,
        validity: vec![0xFF; vals.len().div_ceil(8)],
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1e6
}

struct Stats {
    p50: f64,
    p95: f64,
    min: f64,
}

fn measure(runs: Vec<Duration>) -> Stats {
    let mut sorted: Vec<f64> = runs.into_iter().map(|d| ms(d)).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();
    Stats {
        p50: sorted[n / 2],
        p95: sorted[((n as f64 * 0.95) as usize).min(n - 1)],
        min: sorted[0],
    }
}

fn main() {
    let dir = tempdir().unwrap();
    let n: usize = 1_000_000;

    let ids: Vec<i64> = (0..n as i64).collect();
    let cats: Vec<Vec<u8>> = (0..n)
        .map(|i| match i % 4 {
            0 => b"alpha".to_vec(),
            1 => b"beta".to_vec(),
            2 => b"gamma".to_vec(),
            _ => b"delta".to_vec(),
        })
        .collect();
    let statuses: Vec<Vec<u8>> = (0..n)
        .map(|i| {
            if i % 3 == 0 {
                b"active".to_vec()
            } else {
                b"inactive".to_vec()
            }
        })
        .collect();
    let amounts: Vec<f64> = (0..n).map(|i| 100.0 + (i as f64) * 0.1).collect();
    let timestamps: Vec<i64> = (0..n).map(|i| 1_700_000_000 + i as i64).collect();
    let user_ids: Vec<i64> = (0..n).map(|i| (i / 10) as i64).collect();
    let regions: Vec<Vec<u8>> = (0..n)
        .map(|i| match i % 5 {
            0 => b"north".to_vec(),
            1 => b"south".to_vec(),
            2 => b"east".to_vec(),
            3 => b"west".to_vec(),
            _ => b"central".to_vec(),
        })
        .collect();

    let v = n / 8;
    let mut db = Table::create(dir.path().join("t"), schema(), 1).unwrap();
    db.bulk_load_columns(vec![
        (
            1,
            NativeColumn::Int64 {
                data: ids,
                validity: vec![0xFF; v],
            },
        ),
        (2, mk_bytes_col(&cats)),
        (3, mk_bytes_col(&statuses)),
        (
            4,
            NativeColumn::Float64 {
                data: amounts,
                validity: vec![0xFF; v],
            },
        ),
        (
            5,
            NativeColumn::Int64 {
                data: timestamps,
                validity: vec![0xFF; v],
            },
        ),
        (
            6,
            NativeColumn::Int64 {
                data: user_ids,
                validity: vec![0xFF; v],
            },
        ),
        (7, mk_bytes_col(&regions)),
    ])
    .unwrap();
    db.flush().unwrap();

    let udir = tempdir().unwrap();
    let mut users = Table::create(udir.path().join("u"), users_schema(), 2).unwrap();
    let u_ids: Vec<i64> = (0..100_000).map(|i| i as i64).collect();
    let u_regions: Vec<Vec<u8>> = (0..100_000)
        .map(|i| match i % 5 {
            0 => b"north".to_vec(),
            1 => b"south".to_vec(),
            2 => b"east".to_vec(),
            3 => b"west".to_vec(),
            _ => b"central".to_vec(),
        })
        .collect();
    users
        .bulk_load_columns(vec![
            (
                1,
                NativeColumn::Int64 {
                    data: u_ids,
                    validity: vec![0xFF; 100_000 / 8],
                },
            ),
            (2, mk_bytes_col(&u_regions)),
        ])
        .unwrap();
    users.flush().unwrap();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let session = MongrelSession::new(db);
    rt.block_on(async {
        session.register("events").await.unwrap();
        session.register_db("users", users).await.unwrap();
    });

    // Warmup
    rt.block_on(async {
        let _ = session.run("SELECT count(*) FROM events").await;
    });

    let queries: Vec<(&str, &str)> = vec![
        ("warm_full_count",       "SELECT count(*) FROM events"),
        ("warm_filtered_scan",    "SELECT count(*) FROM events WHERE amount > 500.0"),
        ("warm_agg_group",        "SELECT category, count(*) FROM events GROUP BY category"),
        ("indexed_equality",      "SELECT count(*) FROM events WHERE category = 'beta'"),
        ("sorted_time_window",    "SELECT count(*) FROM events WHERE ts >= 1700000500 AND ts <= 1700001500"),
        ("join_10x",              "SELECT count(*) FROM events e JOIN users u ON e.user_id = u.uid"),
        ("clustered_wide_filter", "SELECT count(*) FROM events WHERE category = 'alpha' AND status = 'active' AND region = 'north'"),
        ("wide_filter",           "SELECT count(*) FROM events WHERE category = 'alpha' AND status = 'active' AND region = 'north' AND amount > 200.0"),
        ("wide_agg",              "SELECT category, status, count(*), avg(amount) FROM events WHERE category = 'alpha' AND status = 'active' AND region = 'north' GROUP BY category, status"),
    ];

    println!("\n┌───────────────────────┬───────────────┬────────┬────────┬───────────────┬────────┬────────┐");
    println!("│         query         │  warm p50 µs  │ p95 µs │ min µs │  cold p50 ms  │ p95 ms │ min ms │");
    println!("├───────────────────────┼───────────────┼────────┼────────┼───────────────┼────────┼────────┤");

    for (name, sql) in &queries {
        let warm: Vec<Duration> = rt.block_on(async {
            let mut times = Vec::new();
            for _ in 0..100 {
                let now = Instant::now();
                let _ = session.run(sql).await;
                times.push(now.elapsed());
            }
            times
        });
        let ws = measure(warm);

        let cold: Vec<Duration> = rt.block_on(async {
            let mut times = Vec::new();
            for _ in 0..20 {
                session.clear_cache();
                let now = Instant::now();
                let _ = session.run(sql).await;
                times.push(now.elapsed());
            }
            times
        });
        let cs = measure(cold);

        println!(
            "│ {:<21} │ {:>13.1} │ {:>6.1} │ {:>6.1} │ {:>13.3} │ {:>6.1} │ {:>6.1} │",
            name, ws.p50, ws.p95, ws.min, cs.p50, cs.p95, cs.min
        );
    }
    println!(
        "└───────────────────────┴───────────────┴──────┴───────┴───────────────┴──────┴───────┘"
    );
    println!("\n(all times in µs, 1M events + 100K users, warm = result-cache hit, cold = cache cleared)");
}
