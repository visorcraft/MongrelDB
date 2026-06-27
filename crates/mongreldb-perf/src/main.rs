//! Cross-engine performance matrix: MongrelDB (plain + encrypted) vs SQLite vs
//! DuckDB (native / Parquet / CSV) at 100 and 1 000 000 rows.
//!
//! Run:  cargo run --release --bin compare
//! (first build compiles bundled SQLite + DuckDB — slow once.)

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, RowId, Value};
use mongreldb_query::MongrelSession;
use std::time::{Duration, Instant};

#[derive(Default, Clone)]
struct Times {
    bulk_insert: Duration,
    single_insert_commit: Duration,
    single_update_commit: Duration,
    delete_one: Duration,
    filter: Duration,
    count: Duration,
    join: Duration,
    // MongrelDB-only: O(1) metadata count + index/tool-call filter (its real
    // query strengths, distinct from the SQL scan path).
    count_meta: Duration,
    filter_toolcall: Duration,
}

fn trips_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef { id: 1, name: "id".into(), ty: TypeId::Int64, flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY) },
            ColumnDef { id: 2, name: "destination".into(), ty: TypeId::Bytes, flags: ColumnFlags::empty() },
            ColumnDef { id: 3, name: "cost".into(), ty: TypeId::Float64, flags: ColumnFlags::empty() },
            ColumnDef { id: 4, name: "ts".into(), ty: TypeId::Int64, flags: ColumnFlags::empty() },
        ],
        indexes: vec![IndexDef { name: "dest_bm".into(), column_id: 2, kind: IndexKind::Bitmap }],
            colocation: vec![],
    }
}

fn cities_schema() -> Schema {
    Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef { id: 1, name: "city_name".into(), ty: TypeId::Bytes, flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY) },
            ColumnDef { id: 2, name: "country".into(), ty: TypeId::Bytes, flags: ColumnFlags::empty() },
        ],
        indexes: vec![],
            colocation: vec![],
    }
}

fn trips_rows(n: i64) -> Vec<Vec<(u16, Value)>> {
    (0..n)
        .map(|i| vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("City{}", i % 50).into_bytes())),
            (3, Value::Float64(199.99 + i as f64)),
            (4, Value::Int64(1_700_000_000 + i)),
        ])
        .collect()
}

fn median(mut ts: Vec<Duration>) -> Duration {
    ts.sort();
    ts[ts.len() / 2]
}

fn us(d: Duration) -> String {
    let s = d.as_secs_f64();
    if s >= 1.0 { format!("{:.2} s", s) } else if s >= 1e-3 { format!("{:.2} ms", s * 1e3) } else { format!("{:.1} µs", s * 1e6) }
}

// ── MongrelDB ─────────────────────────────────────────────────────────────

fn mongrel(n: i64, encrypted: bool) -> Times {
    let dir = tempfile::tempdir().unwrap();
    let mut t = Times::default();

    // bulk_insert: time a fresh bulk_load
    {
        let d = tempfile::tempdir().unwrap();
        let mut db = if encrypted {
            Table::create_encrypted(d.path().join("t"), trips_schema(), 1, "passphrase").unwrap()
        } else {
            Table::create(d.path().join("t"), trips_schema(), 1).unwrap()
        };
        let rows = trips_rows(n);
        let now = Instant::now();
        db.bulk_load(rows).unwrap();
        t.bulk_insert = now.elapsed();
    }

    // load again for the read/update/delete ops
    let d = tempfile::tempdir().unwrap();
    let mut db = if encrypted {
        Table::create_encrypted(d.path().join("t"), trips_schema(), 1, "passphrase").unwrap()
    } else {
        Table::create(d.path().join("t"), trips_schema(), 1).unwrap()
    };
    db.bulk_load(trips_rows(n)).unwrap();

    // MongrelDB-native strengths: O(1) metadata count + index/tool-call filter.
    t.count_meta = median((0..50).map(|_| { let now = Instant::now(); let _ = db.count(); now.elapsed() }).collect());
    t.filter_toolcall = median((0..5).map(|_| {
        let now = Instant::now();
        let q = Query::new().and(Condition::RangeF64 {
            column_id: 3, lo: 200.0, lo_inclusive: false, hi: 250.0, hi_inclusive: true,
        });
        let _ = db.query(&q).unwrap().len();
        now.elapsed()
    }).collect());

    // single-row insert + commit (MongrelDB's sub-ms durable write)
    t.single_insert_commit = median((0..7).map(|i| {
        let now = Instant::now();
        db.put(vec![
            (1, Value::Int64(n + 1 + i)),
            (2, Value::Bytes(b"CityX".to_vec())),
            (3, Value::Float64(1.0)),
            (4, Value::Int64(1)),
        ]).unwrap();
        db.commit().unwrap();
        now.elapsed()
    }).collect());

    // single-row update (put to an existing PK = durable update) + commit
    t.single_update_commit = median((0..7).map(|i| {
        let now = Instant::now();
        db.put(vec![
            (1, Value::Int64(i)), // existing PK → upsert/update
            (2, Value::Bytes(format!("Upd{i}").into_bytes())),
            (3, Value::Float64(99.0 + i as f64)),
            (4, Value::Int64(2)),
        ]).unwrap();
        db.commit().unwrap();
        now.elapsed()
    }).collect());

    // delete one row + commit
    t.delete_one = median((0..7).map(|i| {
        let now = Instant::now();
        db.delete(RowId((n + i) as u64)).unwrap();
        db.commit().unwrap();
        now.elapsed()
    }).collect());

    // SQL filter / count / join via the DataFusion frontend
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cdir = tempfile::tempdir().unwrap();
    let mut cities = Table::create(cdir.path().join("c"), cities_schema(), 2).unwrap();
    cities.bulk_load(
        (0..50i64).map(|i| vec![
            (1, Value::Bytes(format!("City{i}").into_bytes())),
            (2, Value::Bytes(if i % 2 == 0 { b"North" } else { b"South" }.to_vec())),
        ]).collect(),
    ).unwrap();
    let session = MongrelSession::new(db);
    rt.block_on(async {
        session.register("trips").await.unwrap();
        session.register_db("cities", cities).await.unwrap();

        let mut ts;
        ts = vec![];
        for _ in 0..5 {
            session.clear_cache(); // measure cold query time, not the result cache
            let now = Instant::now();
            session.run("select id from trips where cost < 250.0").await.unwrap();
            ts.push(now.elapsed());
        }
        t.filter = median(ts);

        ts = vec![];
        for _ in 0..20 {
            session.clear_cache();
            let now = Instant::now();
            session.run("select count(*) from trips").await.unwrap();
            ts.push(now.elapsed());
        }
        t.count = median(ts);

        ts = vec![];
        for _ in 0..5 {
            session.clear_cache();
            let now = Instant::now();
            session.run("select count(*) from trips t join cities c on t.destination = c.city_name")
                .await
                .unwrap();
            ts.push(now.elapsed());
        }
        t.join = median(ts);
    });
    std::mem::forget(cdir); // keep cities dir alive for join
    t
}

// ── SQLite ────────────────────────────────────────────────────────────────

fn sqlite(n: i64) -> Times {
    use rusqlite::Connection;
    let dir = tempfile::tempdir().unwrap();
    let mut t = Times::default();

    // bulk_insert (transaction + prepared)
    {
        let conn = Connection::open(dir.path().join("s.db")).unwrap();
        conn.execute("CREATE TABLE trips (id INTEGER PRIMARY KEY, destination TEXT, cost REAL, ts INTEGER)", []).unwrap();
        let rows = trips_rows(n);
        let now = Instant::now();
        conn.execute_batch("BEGIN").unwrap();
        let mut stmt = conn.prepare("INSERT INTO trips VALUES (?,?,?,?)").unwrap();
        for r in &rows {
            let dest: &[u8] = match &r[1].1 { Value::Bytes(b) => b, _ => b"" };
            let cost = match r[2].1 { Value::Float64(f) => f, _ => 0.0 };
            let ts = match r[3].1 { Value::Int64(v) => v, _ => 0 };
            stmt.execute(rusqlite::params![match r[0].1 { Value::Int64(v) => v, _ => 0 }, dest, cost, ts]).unwrap();
        }
        drop(stmt);
        conn.execute_batch("COMMIT").unwrap();
        t.bulk_insert = now.elapsed();

        t.single_insert_commit = median((0..7).map(|i| {
            let now = Instant::now();
            conn.execute("INSERT INTO trips VALUES (?,?,?,?)", rusqlite::params![n + 1 + i, "CityX", 1.0, 1]).unwrap();
            now.elapsed()
        }).collect());

        t.single_update_commit = median((0..7).map(|i| {
            let now = Instant::now();
            conn.execute(
                "UPDATE trips SET destination=?, cost=?, ts=? WHERE id=?",
                rusqlite::params![format!("Upd{i}"), 99.0 + i as f64, 2, i],
            ).unwrap();
            now.elapsed()
        }).collect());

        conn.execute("CREATE TABLE cities (city_name TEXT PRIMARY KEY, country TEXT)", []).unwrap();
        conn.execute_batch("BEGIN").unwrap();
        for i in 0..50i64 {
            conn.execute("INSERT INTO cities VALUES (?,?)", rusqlite::params![format!("City{i}"), if i % 2 == 0 { "North" } else { "South" }]).unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();

        t.delete_one = median((0..7).map(|i| {
            let now = Instant::now();
            conn.execute("DELETE FROM trips WHERE id=?", [(n + i)]).unwrap();
            now.elapsed()
        }).collect());

        t.filter = median((0..5).map(|_| {
            let now = Instant::now();
            let c: i64 = conn.query_row("SELECT COUNT(*) FROM trips WHERE cost < 250.0", [], |r| r.get(0)).unwrap();
            let _ = c; now.elapsed()
        }).collect());
        t.count = median((0..20).map(|_| {
            let now = Instant::now();
            let _: i64 = conn.query_row("SELECT COUNT(*) FROM trips", [], |r| r.get(0)).unwrap();
            now.elapsed()
        }).collect());
        t.join = median((0..5).map(|_| {
            let now = Instant::now();
            let _: i64 = conn.query_row("SELECT COUNT(*) FROM trips t JOIN cities c ON t.destination=c.city_name", [], |r| r.get(0)).unwrap();
            now.elapsed()
        }).collect());
    }
    let _ = dir; t
}

// ── DuckDB (native + file-backed) ─────────────────────────────────────────

fn duckdb_load(conn: &duckdb::Connection, n: i64) {
    conn.execute("CREATE TABLE trips (id BIGINT, destination VARCHAR, cost DOUBLE, ts BIGINT)", []).unwrap();
    let rows = trips_rows(n);
    conn.execute_batch("BEGIN").unwrap();
    let mut app = conn.appender("trips").unwrap();
    for r in &rows {
        let id = match r[0].1 { Value::Int64(v) => v, _ => 0 };
        let dest = std::str::from_utf8(match &r[1].1 { Value::Bytes(b) => b, _ => b"" }).unwrap_or("");
        let cost = match r[2].1 { Value::Float64(f) => f, _ => 0.0 };
        let ts = match r[3].1 { Value::Int64(v) => v, _ => 0 };
        app.append_row((id, dest, cost, ts)).unwrap();
    }
    drop(app);
    conn.execute_batch("COMMIT").unwrap();
}

fn duckdb_cities(conn: &duckdb::Connection) {
    conn.execute("CREATE TABLE cities (city_name VARCHAR, country VARCHAR)", []).unwrap();
    conn.execute_batch("BEGIN").unwrap();
    let mut app = conn.appender("cities").unwrap();
    for i in 0..50i64 {
        let city = format!("City{i}");
        let country = if i % 2 == 0 { "North" } else { "South" };
        app.append_row((city.as_str(), country)).unwrap();
    }
    drop(app);
    conn.execute_batch("COMMIT").unwrap();
}

fn duckdb_count(conn: &duckdb::Connection, sql: &str) -> i64 {
    let mut stmt = conn.prepare(sql).unwrap();
    let mut rows = stmt.query([]).unwrap();
    rows.next().unwrap().unwrap().get::<_, i64>(0).unwrap()
}

fn duckdb(n: i64, format: Option<&str>) -> Times {
    let dir = tempfile::tempdir().unwrap();
    let mut t = Times::default();
    let conn = duckdb::Connection::open(dir.path().join("d.db")).unwrap();

    // load into a native table first
    let now = Instant::now();
    duckdb_load(&conn, n);
    let native_load = now.elapsed();

    if let Some(fmt) = format {
        // File-backed engine: COPY the native table out, then read from the file.
        let ext = if fmt == "parquet" { "parquet" } else { "csv" };
        let path = dir.path().join(format!("trips.{ext}"));
        let now = Instant::now();
        conn.execute_batch(&format!(
            "COPY trips TO '{}' (FORMAT {})",
            path.display(), fmt.to_uppercase()
        )).unwrap();
        t.bulk_insert = now.elapsed(); // load = file creation

        let rconn = duckdb::Connection::open(dir.path().join("d2.db")).unwrap();
        let view = if fmt == "parquet" {
            format!("CREATE VIEW trips AS SELECT * FROM read_parquet('{}')", path.display())
        } else {
            format!("CREATE VIEW trips AS SELECT * FROM read_csv_auto('{}')", path.display())
        };
        rconn.execute_batch(&view).unwrap();
        duckdb_cities(&rconn);
        t.filter = median((0..5).map(|_| { let now = Instant::now(); let _ = duckdb_count(&rconn, "SELECT COUNT(*) FROM trips WHERE cost < 250.0"); now.elapsed() }).collect());
        t.count = median((0..20).map(|_| { let now = Instant::now(); let _ = duckdb_count(&rconn, "SELECT COUNT(*) FROM trips"); now.elapsed() }).collect());
        t.join = median((0..5).map(|_| { let now = Instant::now(); let _ = duckdb_count(&rconn, "SELECT COUNT(*) FROM trips t JOIN cities c ON t.destination=c.city_name"); now.elapsed() }).collect());
    } else {
        t.bulk_insert = native_load;
        duckdb_cities(&conn);
        t.single_insert_commit = median((0..7).map(|i| {
            let now = Instant::now();
            conn.execute("INSERT INTO trips VALUES (?,?,?,?)", duckdb::params![n + 1 + i, "CityX", 1.0, 1]).unwrap();
            now.elapsed()
        }).collect());
        t.single_update_commit = median((0..7).map(|i| {
            let now = Instant::now();
            conn.execute(
                "UPDATE trips SET destination=?, cost=?, ts=? WHERE id=?",
                duckdb::params![format!("Upd{i}"), 99.0 + i as f64, 2, i],
            ).unwrap();
            now.elapsed()
        }).collect());
        t.delete_one = median((0..7).map(|i| {
            let now = Instant::now();
            conn.execute("DELETE FROM trips WHERE id=?", [(n + i)]).unwrap();
            now.elapsed()
        }).collect());
        t.filter = median((0..5).map(|_| { let now = Instant::now(); let _ = duckdb_count(&conn, "SELECT COUNT(*) FROM trips WHERE cost < 250.0"); now.elapsed() }).collect());
        t.count = median((0..20).map(|_| { let now = Instant::now(); let _ = duckdb_count(&conn, "SELECT COUNT(*) FROM trips"); now.elapsed() }).collect());
        t.join = median((0..5).map(|_| { let now = Instant::now(); let _ = duckdb_count(&conn, "SELECT COUNT(*) FROM trips t JOIN cities c ON t.destination=c.city_name"); now.elapsed() }).collect());
    }
    let _ = dir; t
}

// ── output ────────────────────────────────────────────────────────────────

fn row(label: &str, t: &Times, cols: &[&str]) -> Vec<String> {
    let mut v = vec![label.to_string()];
    for c in cols {
        let d = match *c {
            "bulk_insert" => t.bulk_insert,
            "single_insert_commit" => t.single_insert_commit,
            "single_update_commit" => t.single_update_commit,
            "delete_one" => t.delete_one,
            "filter(cost<250)" => t.filter,
            "count_star" => t.count,
            "join(cities)" => t.join,
            _ => Duration::ZERO,
        };
        let s = if d == Duration::ZERO { "—".into() } else { us(d) };
        v.push(s);
    }
    v
}

fn table(title: &str, cols: &[&str], rows: &[Vec<String>]) {
    println!("### {title}\n");
    print!("| engine |");
    for c in cols { print!(" {c} |"); }
    println!("\n|---|{}", cols.iter().map(|_| "---:|").collect::<String>());
    for r in rows {
        print!("|");
        for cell in r { print!(" {cell} |"); }
        println!();
    }
    println!();
}

fn main() {
    let only_mdb = std::env::args().any(|a| a == "mongrel");
    let title = if only_mdb {
        "MongrelDB performance matrix"
    } else {
        "MongrelDB cross-engine performance matrix"
    };
    println!("{title}\n");
    if !only_mdb {
        println!("Notes: all engines embedded/in-process (no daemon). MongrelDB and the\n\
                  DataFusion session keep in-memory indexes + a per-(sql,epoch) result cache;\n\
                  SQLite/DuckDB use their own page caches. MongrelDB `count` is O(1) metadata;\n\
                  others scan. `filter`/`join` go through each engine's SQL planner. Parquet/CSV\n\
                  are immutable so single-row insert/delete are N/A (load = file write).\n");
    } else {
        println!("Notes: in-process (no daemon). In-memory indexes + a per-(sql,epoch) result\n\
                  cache (disabled via clear_cache for cold queries). `count` is O(1) metadata;\n\
                  `count_star` is SELECT COUNT(*) (scan). `filter`/`join` go through the\n\
                  DataFusion SQL planner.\n");
    }

    let cols = ["bulk_insert", "single_insert_commit", "single_update_commit", "delete_one", "filter(cost<250)", "count_star", "join(cities)"];

    for &n in &[100i64, 1_000_000] {
        let m = mongrel(n, false);
        let me = mongrel(n, true);

        let mut rows = vec![];
        rows.push(row("MongrelDB", &m, &cols));
        rows.push(row("MongrelDB (enc)", &me, &cols));
        if !only_mdb {
            rows.push(row("SQLite (rusqlite)", &sqlite(n), &cols));
            rows.push(row("DuckDB native", &duckdb(n, None), &cols));
            rows.push(row("DuckDB-Parquet", &duckdb(n, Some("parquet")), &cols));
            rows.push(row("DuckDB-CSV", &duckdb(n, Some("csv")), &cols));
        }
        table(&format!("N = {n} rows (median of runs)"), &cols, &rows);
    }

    println!("### Encryption overhead (MongrelDB, plain vs AES-256-GCM)\n");
    let cols2 = ["bulk_insert", "filter(cost<250)", "count_star", "join(cities)"];
    for &n in &[100i64, 1_000_000] {
        let m = mongrel(n, false);
        let me = mongrel(n, true);
        let mut rows = vec![];
        rows.push(row("plain", &m, &cols2));
        rows.push(row("encrypted", &me, &cols2));
        table(&format!("N = {n}"), &cols2, &rows);
    }

    println!("### MongrelDB native query paths (not the SQL scan path)\n");
    println!("| N | count() O(1) metadata | filter via Table::query (index/tool-call) |");
    println!("|---:|---:|---:|");
    for &n in &[100i64, 1_000_000] {
        let m = mongrel(n, false);
        println!("| {n} | {} | {} |", us(m.count_meta), us(m.filter_toolcall));
    }
    println!();
}

fn storage_efficiency() {
    use std::fs;
    let dir = tempfile::tempdir().unwrap();
    let mut db = Table::create(dir.path().join("t"), trips_schema(), 1).unwrap();
    let rows = trips_rows(1_000_000);
    db.bulk_load(rows).unwrap();
    db.flush().unwrap();
    let mut total: u64 = 0;
    for e in fs::read_dir(dir.path().join("t").join("_runs")).unwrap().flatten() {
        total += e.metadata().unwrap().len();
    }
    println!("Storage: {:.2} bytes/row ({:.2} MB for 1M rows, 4 columns)\n",
             total as f64 / 1e6, total as f64 / 1e6 / 1e6 * 1e6 / 1e6);
}
