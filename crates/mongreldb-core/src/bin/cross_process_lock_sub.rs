//! Sub-binary for `tests/cross_process_lock.rs`.
//!
//! Roles:
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
//! - `create_then_exit <dir>`: runs `Database::create` then exits. Used to
//!   pre-create a DB in a subprocess so the parent's per-process
//!   `LOCKED_PATHS` skip doesn't mask the cross-process contention these
//!   tests are designed to observe.
//!
//! - `delayed_creator <dir>`: reserves the pre-open sidecar lock before the
//!   database directory exists, then creates after a short delay. Used to
//!   prove a losing concurrent creator cannot touch the database directory.
//!
//! - `auth_enabler <dir>`: opens a credentialless database, reports readiness,
//!   enables auth, and exits. Used to prove a waiting open reads the catalog
//!   only after it owns the lock.
//!
//! - `writer <dir> <writer_id> <lock_timeout_ms> <rows>`: opens the
//!   database with a configurable cross-process lock timeout, commits
//!   `rows` transactions on the `items` table (each transaction inserts
//!   one row keyed by `(writer_id, idx)`), and exits. Used by
//!   `tests/cross_process_lock.rs::cross_process_writers_all_succeed` to
//!   exercise the new `OpenOptions::lock_timeout_ms` knob with real
//!   parallel writers.
//!
//! Built automatically as a bin target; located at
//! `$CARGO_BIN_EXE_cross_process_lock_sub`.

use std::io::Write;
use std::process::exit;
use std::time::{Duration, Instant};

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Database;
use mongreldb_core::OpenOptions as CoreOpenOptions;
use mongreldb_core::Value;

fn spin_forever(started: Instant) -> ! {
    loop {
        if started.elapsed() > Duration::from_secs(60) {
            eprintln!("holder timed out, exiting");
            exit(3);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn writer_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "writer".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "idx".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 3,
                name: "payload".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn bootstrap_lock_path(root: &std::path::Path) -> std::path::PathBuf {
    use std::hash::{Hash, Hasher};

    let parent = root
        .parent()
        .expect("database root parent")
        .canonicalize()
        .unwrap();
    let canonical = parent.join(root.file_name().expect("database root name"));
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    parent.join(format!(".mongreldb-{:016x}.lock", hasher.finish()))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let role = match args.next() {
        Some(r) => r,
        None => {
            eprintln!(
                "usage: cross_process_lock_sub <flock_holder|engine_holder|create_then_exit|delayed_creator|auth_enabler|writer> \
                 <dir> [writer_args...]"
            );
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
            let path = std::path::Path::new(&dir);
            let db = Database::create(path).expect("create then exit");
            // Ensure the `items` table exists so writers have a known schema.
            // `create_table` errors with `InvalidArgument` if it already does;
            // that's fine because `create_then_exit` may be re-run against a
            // populated directory in some tests.
            match db.create_table("items", writer_schema()) {
                Ok(_) => {}
                Err(mongreldb_core::MongrelError::InvalidArgument(msg))
                    if msg.contains("already exists") => {}
                Err(e) => panic!("create items table: {e}"),
            }
            drop(db);
            println!("CREATED");
            std::io::stdout().flush().expect("flush CREATED");
            exit(0);
        }
        "delayed_creator" => {
            use fs2::FileExt;

            let path = std::path::Path::new(&dir);
            let lock_path = bootstrap_lock_path(path);
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(lock_path)
                .expect("open bootstrap lock");
            file.lock_exclusive().expect("acquire bootstrap lock");
            println!("READY");
            std::io::stdout().flush().expect("flush READY");
            std::thread::sleep(Duration::from_millis(500));
            fs2::FileExt::unlock(&file).expect("release bootstrap lock");
            drop(file);
            let db = Database::create(path).expect("delayed create");
            drop(db);
            println!("CREATED");
            std::io::stdout().flush().expect("flush CREATED");
            exit(0);
        }
        "auth_enabler" => {
            let path = std::path::Path::new(&dir);
            let db = Database::open(path).expect("auth_enabler open");
            println!("READY");
            std::io::stdout().flush().expect("flush READY");
            std::thread::sleep(Duration::from_millis(300));
            db.enable_auth("admin", "admin-password")
                .expect("enable auth");
            drop(db);
            println!("DONE");
            std::io::stdout().flush().expect("flush DONE");
            exit(0);
        }
        "engine_holder" => {
            let path = std::path::Path::new(&dir);
            let db = Database::open(path).expect("engine_holder acquire lock");
            let _ = db.catalog_snapshot();
            println!("READY");
            std::io::stdout().flush().expect("flush READY");
            spin_forever(Instant::now());
        }
        "writer" => {
            // writer <dir> <writer_id> <lock_timeout_ms> <rows>
            let writer_id: i64 = args
                .next()
                .expect("missing writer_id")
                .parse()
                .expect("writer_id must be i64");
            let lock_timeout_ms: u32 = args
                .next()
                .expect("missing lock_timeout_ms")
                .parse()
                .expect("lock_timeout_ms must be u32");
            let rows: i64 = args
                .next()
                .expect("missing rows")
                .parse()
                .expect("rows must be i64");

            let path = std::path::Path::new(&dir);
            let opts = CoreOpenOptions::default().with_lock_timeout_ms(lock_timeout_ms);
            let db = Database::open_with_options(path, opts).expect("writer open");
            let table = db.table("items").expect("items table missing").clone();

            for idx in 0..rows {
                let payload = format!("w{writer_id}-r{idx}");
                let mut guard = table.lock();
                guard
                    .put(vec![
                        (1, Value::Int64(writer_id)),
                        (2, Value::Int64(writer_id * rows + idx)),
                        (3, Value::Bytes(payload.into_bytes())),
                    ])
                    .expect("put");
                guard.commit().expect("commit");
            }

            println!("DONE {writer_id}");
            std::io::stdout().flush().expect("flush DONE");
            exit(0);
        }
        other => {
            eprintln!("unknown role: {other}");
            exit(2);
        }
    }
}
