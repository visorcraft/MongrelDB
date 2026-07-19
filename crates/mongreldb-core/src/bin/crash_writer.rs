//! Child-process writer for the real `kill -9` crash harness
//! (`tests/crash_process.rs`). Invoked as:
//!
//! ```text
//! crash_writer <db_dir> <mode>
//! ```
//!
//! Each mode drives the engine to a known pre-crash state, prints a readiness
//! line to stdout (flushed), then spins forever so the parent test can deliver
//! `SIGKILL` at a well-defined point. The parent reopens the DB afterwards and
//! asserts the crash-recovery invariant.
//!
//! Built automatically by `cargo test` (it is a bin target of this package);
//! located at `$CARGO_BIN_EXE_crash_writer`.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{Database, Table, Value};
use std::io::Write;
use std::process::exit;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            default_value: None,
            embedding_source: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (dir, mode) = match (args.next(), args.next()) {
        (Some(d), Some(m)) => (std::path::PathBuf::from(d), m),
        _ => {
            eprintln!("usage: crash_writer <db_dir> <mode>");
            exit(2);
        }
    };

    match mode.as_str() {
        // A single committed row. The durability point (`commit()` → WAL fsync +
        // manifest epoch advance) completes before we signal, so the row must
        // survive a `kill -9` that lands any time after COMMITTED_READY.
        "committed" => {
            let mut db = Table::create(&dir, schema(), 1).expect("create");
            db.put(vec![(1, Value::Int64(4242))]).expect("put");
            db.commit().expect("commit");
            signal("COMMITTED_READY");
            spin();
        }
        // Several independent committed transactions (distinct PKs) — all must
        // survive a `kill -9` after the final commit.
        "committed-burst" => {
            let mut db = Table::create(&dir, schema(), 1).expect("create");
            for v in [10i64, 20, 30] {
                db.put(vec![(1, Value::Int64(v))]).expect("put");
                db.commit().expect("commit");
            }
            signal("BURST_READY");
            spin();
        }
        // A row whose WAL `Put` record is fsynced to disk, but whose transaction
        // is never committed. We pin the auto-sync threshold to 1 byte so the
        // append fsyncs immediately, then never call `commit()` — leaving a
        // durable-but-uncommitted record. Recovery must replay it into the
        // memtable yet keep it invisible (committed_epoch > manifest epoch), and
        // must not panic or corrupt.
        "uncommitted" => {
            let mut db = Table::create(&dir, schema(), 1).expect("create");
            db.set_sync_byte_threshold(1);
            db.put(vec![(1, Value::Int64(9999))]).expect("put");
            signal("UNCOMMITTED_READY");
            spin();
        }
        // A row that is fully `flush()`ed: committed, spilled to an immutable
        // `.sr` sorted run (spill watermark pinned to 1 byte), WAL rotated. The
        // crash lands after the run is on disk and the WAL is rotated, so
        // recovery must read the row from the run.
        "flush-spill" => {
            let mut db = Table::create(&dir, schema(), 1).expect("create");
            db.set_mutable_run_spill_bytes(1);
            db.put(vec![(1, Value::Int64(7777))]).expect("put");
            db.flush().expect("flush");
            signal("FLUSH_READY");
            spin();
        }
        // A fully loaded but unpublished CTAS build. It is durable, hidden,
        // and must be reclaimed when the database reopens after the kill.
        "ctas-building" => {
            let db = Database::create(&dir).expect("create database");
            let build = "__mongreldb_ctas_build_crash-query";
            db.create_building_table(build, "target", "crash-query", schema())
                .expect("create building table");
            let mut txn = db.begin();
            txn.put_building(build, vec![(1, Value::Int64(4242))])
                .expect("put building row");
            txn.commit().expect("commit building row");
            signal("CTAS_BUILDING_READY");
            spin();
        }
        // Shared-WAL (mounted-table) path: open or create a database, create
        // the table on first use, commit one row carrying this session's
        // value, then spin. The parent kills with SIGKILL after SHARED_READY
        // and reopens — exercising the multi-table shared WAL recovery path
        // where every process stop is an unclean shutdown.
        "shared-commit" => {
            let value: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(1);
            let db = match Database::open(&dir) {
                Ok(db) => db,
                Err(_) => Database::create(&dir).expect("create database"),
            };
            if db.table("t").is_err() {
                db.create_table("t", schema()).expect("create table");
            }
            let t = db.table("t").expect("table");
            {
                let mut g = t.lock();
                g.put(vec![(1, Value::Int64(value))]).expect("put");
                g.commit().expect("commit");
            }
            signal("SHARED_READY");
            spin();
        }
        // Encrypted variant of "shared-commit": same restart semantics must
        // hold with page-level AES-256-GCM. Fixed test-only passphrase.
        "shared-commit-encrypted" => {
            const PASSPHRASE: &str = "crash-harness-test-passphrase";
            let value: i64 = args.next().and_then(|v| v.parse().ok()).unwrap_or(1);
            let db = match Database::open_encrypted(&dir, PASSPHRASE) {
                Ok(db) => db,
                Err(_) => Database::create_encrypted(&dir, PASSPHRASE).expect("create database"),
            };
            if db.table("t").is_err() {
                db.create_table("t", schema()).expect("create table");
            }
            let t = db.table("t").expect("table");
            {
                let mut g = t.lock();
                g.put(vec![(1, Value::Int64(value))]).expect("put");
                g.commit().expect("commit");
            }
            signal("SHARED_READY");
            spin();
        }
        // Stop inside one canonical durable hook. The parent kills this
        // process while the hook callback holds the exact boundary, then
        // reopens and validates the published or recoverable state.
        "durable-hook" => {
            let hook = args.next().unwrap_or_else(|| {
                eprintln!("durable-hook requires a hook name");
                exit(2);
            });
            let hook = durable_hook(&hook);
            match hook {
                "wal.append.before"
                | "wal.append.after"
                | "wal.fsync.before"
                | "wal.fsync.after"
                | "commit.publish.before"
                | "commit.publish.after" => {
                    let db = database_with_row(&dir);
                    arm_crash_hook(hook);
                    let mut transaction = db.begin();
                    transaction
                        .put("items", vec![(1, Value::Int64(2))])
                        .expect("stage hook row");
                    let _ = transaction.commit();
                }
                "catalog.publish.before" | "catalog.publish.after" => {
                    let db = database_with_row(&dir);
                    arm_crash_hook(hook);
                    let _ = db.create_table("published", schema());
                }
                "snapshot.install.before" | "snapshot.install.after" => {
                    let leader = database_with_row(&dir.join("leader"));
                    let snapshot = leader.replication_snapshot().expect("snapshot");
                    arm_crash_hook(hook);
                    let _ = snapshot.install(dir.join("follower"));
                }
                "index.publish.before" | "index.publish.after" => {
                    let db = database_with_row(&dir);
                    db.table("items")
                        .expect("items")
                        .lock()
                        .set_mutable_run_spill_bytes(1);
                    arm_crash_hook(hook);
                    let _ = db.table("items").expect("items").lock().flush();
                }
                _ => unreachable!(),
            }
            panic!("durable hook {hook} was not hit");
        }
        other => {
            eprintln!("unknown mode: {other}");
            exit(2);
        }
    }
}

fn database_with_row(path: &std::path::Path) -> Database {
    let db = Database::create(path).expect("create database");
    db.create_table("items", schema()).expect("create table");
    db.transaction(|transaction| {
        transaction.put("items", vec![(1, Value::Int64(1))])?;
        Ok(())
    })
    .expect("commit baseline row");
    db
}

fn durable_hook(name: &str) -> &'static str {
    match name {
        "wal.append.before" => "wal.append.before",
        "wal.append.after" => "wal.append.after",
        "wal.fsync.before" => "wal.fsync.before",
        "wal.fsync.after" => "wal.fsync.after",
        "commit.publish.before" => "commit.publish.before",
        "commit.publish.after" => "commit.publish.after",
        "catalog.publish.before" => "catalog.publish.before",
        "catalog.publish.after" => "catalog.publish.after",
        "snapshot.install.before" => "snapshot.install.before",
        "snapshot.install.after" => "snapshot.install.after",
        "index.publish.before" => "index.publish.before",
        "index.publish.after" => "index.publish.after",
        other => {
            eprintln!("unknown durable hook: {other}");
            exit(2);
        }
    }
}

fn arm_crash_hook(hook: &'static str) {
    mongreldb_fault::activate(
        hook,
        mongreldb_fault::Action::Callback(Arc::new(|_| {
            signal("HOOK_READY");
            spin();
        })),
    );
}

fn signal(msg: &str) {
    println!("{msg}");
    let _ = std::io::stdout().flush();
}

/// Spin so the parent can deliver `SIGKILL`. Bounded wall-clock as a safety net
/// so a stray manual invocation won't hang indefinitely.
fn spin() {
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
}
