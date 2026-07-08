//! Sub-binary for `tests/cross_process_lock.rs`.
//!
//! Two roles:
//!
//! - `flock_holder <dir>`: open `<dir>/_meta/.lock` and immediately acquire
//!   an exclusive `flock(2)` on it (via `fs2`); print `READY` (flushed)
//!   once held, then spin forever (with a 60s belt-and-suspenders
//!   timeout) until `SIGKILL`. The test parent uses this to simulate
//!   "another process holds the database lock" without depending on a
//!   fully-formed engine state.
//!
//! - `engine_holder <dir>`: open the mongreldb database at `<dir>` and
//!   hold it for the same kind of harness. Same purpose but exercises the
//!   full engine mount path as a sanity check that the cross-process
//!   lock via the engine itself works the same way.
//!
//! Built automatically as a bin target; located at
//! `$CARGO_BIN_EXE_cross_process_lock_sub`.

use std::io::Write;
use std::process::exit;
use std::time::{Duration, Instant};

use mongreldb_core::Database;

fn spin_forever(started: Instant) -> ! {
    loop {
        if started.elapsed() > Duration::from_secs(60) {
            eprintln!("holder timed out, exiting");
            exit(3);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let role = match args.next() {
        Some(r) => r,
        None => {
            eprintln!("usage: cross_process_lock_sub <flock_holder|engine_holder> <dir>");
            exit(2);
        }
    };
    let dir = match args.next() {
        Some(d) => d,
        None => {
            eprintln!("missing <dir> argument");
            exit(2);
        }
    };

    match role.as_str() {
        "flock_holder" => {
            use fs2::FileExt;
            let lock_path = std::path::PathBuf::from(&dir).join("_meta").join(".lock");
            std::fs::create_dir_all(lock_path.parent().unwrap()).expect("create _meta dir");
            let f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&lock_path)
                .expect("open lock file");
            f.lock_exclusive().expect("acquire exclusive flock");
            println!("READY");
            std::io::stdout().flush().expect("flush READY");
            spin_forever(Instant::now());
        }
        "create_then_exit" => {
            // Pre-create the database in a separate process so the parent's
            // per-process `LOCKED_PATHS` skip doesn't mask the cross-process
            // contention we're trying to test. Used by `tests/cross_process_lock.rs`.
            let path = std::path::Path::new(&dir);
            let _db = Database::create(path).expect("create then exit");
            // Drop `_db` so the OS flock is released before the parent's
            // test opens. We exit immediately after; the parent takes over.
            drop(_db);
            println!("CREATED");
            std::io::stdout().flush().expect("flush CREATED");
            exit(0);
        }
        "engine_holder" => {
            let path = std::path::Path::new(&dir);
            let db = Database::open(path).expect("engine_holder acquire lock");
            // Cheap liveness probe to keep the engine mount fully exercised.
            let _ = db.catalog_snapshot();
            println!("READY");
            std::io::stdout().flush().expect("flush READY");
            spin_forever(Instant::now());
        }
        other => {
            eprintln!("unknown role: {other}");
            exit(2);
        }
    }
}
