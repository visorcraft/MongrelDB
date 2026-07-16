//! True process-level crash harness: spawns the `crash_writer` bin, waits for it
//! to reach a known pre-crash state, delivers `SIGKILL` (`kill -9`), then reopens
//! the DB and asserts the recovery invariant. Unlike the simulated crash tests
//! in `crash.rs` (which manipulate on-disk files directly), these tests kill a
//! *real* child process mid-run — exercising the actual OS-level crash path
//! (kernel reaping the process, unclosed file handles, page-cache state).
//!
//! On Unix, `Child::kill` is documented to send `SIGKILL`, i.e. `kill -9`.

use mongreldb_core::{Database, Table, Value};
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use tempfile::TempDir;

/// Locate the `crash_writer` bin. Cargo sets `CARGO_BIN_EXE_<name>` for
/// integration tests of the same package; fall back to a path relative to the
/// running test binary for direct invocation.
fn writer_exe() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_crash_writer") {
        return PathBuf::from(p);
    }
    let exe = std::env::current_exe().expect("current_exe");
    exe.parent()
        .and_then(|deps| deps.parent())
        .map(|target| target.join("crash_writer"))
        .expect("locate crash_writer")
}

/// Spawn `crash_writer <dir> <mode>`, wait until it prints `ready`, then kill
/// it with `SIGKILL`. Panics if the writer exits before signalling or if the
/// readiness line does not arrive within the timeout. `dir` (the tempdir) is
/// owned by the caller so the on-disk state outlives the kill for reopening.
fn spawn_and_kill(mode: &str, ready: &str, db_dir: &std::path::Path) {
    let mut child = Command::new(writer_exe())
        .arg(db_dir)
        .arg(mode)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn crash_writer");

    let stdout = child.stdout.take().expect("piped stdout");
    // Read lines on a thread so the main thread can apply a timeout (a blocking
    // read on a pipe has no inherent deadline). When the child is killed below,
    // its stdout pipe closes and the thread's iterator ends naturally.
    let (tx, rx) = mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if tx.send(line).is_err() {
                break;
            }
        }
    });

    let mut saw_ready = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while std::time::Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(line) if line == ready => {
                saw_ready = true;
                break;
            }
            Ok(other) => eprintln!("[crash_writer] {other}"),
            Err(mpsc::RecvTimeoutError::Timeout) => break,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    // Kill -9 the child regardless of outcome, then reap.
    let kill_ok = child.kill().is_ok();
    let _ = child.wait();
    let _ = reader.join();

    assert!(
        saw_ready,
        "writer never signalled {ready} (killed={kill_ok})"
    );
}

fn values(db: &Table) -> Vec<i64> {
    let rows = db.visible_rows(db.snapshot()).expect("visible_rows");
    let mut vals: Vec<i64> = rows
        .iter()
        .filter_map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => Some(*v),
            _ => None,
        })
        .collect();
    vals.sort_unstable();
    vals
}

#[test]
fn committed_row_survives_real_kill9() {
    let dir = TempDir::new().unwrap();
    spawn_and_kill("committed", "COMMITTED_READY", dir.path());

    let db = Table::open(dir.path()).expect("reopen after kill -9");
    assert_eq!(
        values(&db),
        vec![4242],
        "committed row must survive kill -9"
    );
    assert_eq!(db.count(), 1);
}

#[test]
fn committed_burst_survives_real_kill9() {
    let dir = TempDir::new().unwrap();
    spawn_and_kill("committed-burst", "BURST_READY", dir.path());

    let db = Table::open(dir.path()).expect("reopen after kill -9");
    assert_eq!(
        values(&db),
        vec![10, 20, 30],
        "all committed rows must survive kill -9"
    );
    assert_eq!(db.count(), 3);
}

#[test]
fn uncommitted_row_stays_invisible_after_real_kill9() {
    let dir = TempDir::new().unwrap();
    spawn_and_kill("uncommitted", "UNCOMMITTED_READY", dir.path());

    // Recovery must not panic or corrupt, even though the WAL `Put` record was
    // fsynced to disk; the uncommitted row is replayed into the memtable but
    // hidden by MVCC (committed_epoch > manifest epoch).
    let db = Table::open(dir.path()).expect("reopen after kill -9");
    assert!(
        values(&db).is_empty(),
        "uncommitted row must not be visible after kill -9"
    );
    assert_eq!(db.count(), 0);
}

#[test]
fn flushed_run_survives_real_kill9() {
    let dir = TempDir::new().unwrap();
    spawn_and_kill("flush-spill", "FLUSH_READY", dir.path());

    let db = Table::open(dir.path()).expect("reopen after kill -9");
    assert_eq!(
        values(&db),
        vec![7777],
        "flushed (run + rotated WAL) row must survive kill -9"
    );
    assert_eq!(db.count(), 1);
}

#[test]
fn unpublished_ctas_build_is_reclaimed_after_real_kill9() {
    let dir = TempDir::new().unwrap();
    spawn_and_kill("ctas-building", "CTAS_BUILDING_READY", dir.path());

    let db = Database::open(dir.path()).expect("reopen database after kill -9");
    assert!(db.table_names().is_empty());
    assert!(db.table("target").is_err());
    assert!(db.table("__mongreldb_ctas_build_crash-query").is_err());
}
