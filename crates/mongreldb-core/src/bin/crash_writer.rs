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
use mongreldb_core::{Table, Value};
use std::io::Write;
use std::process::exit;
use std::time::{Duration, Instant};

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "v".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
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
        other => {
            eprintln!("unknown mode: {other}");
            exit(2);
        }
    }
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
