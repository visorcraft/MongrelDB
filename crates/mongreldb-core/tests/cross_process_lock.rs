//! Cross-process file lock tests for the new `OpenOptions::lock_timeout_ms`
//! knob.
//!
//! Every test follows the same shape:
//!
//! 1. `create_then_exit`: spawn a subprocess that runs `Database::create`
//!    and exits. This produces a populated `<dir>` and is the only way to
//!    pre-create the DB without polluting the parent's per-process
//!    `LOCKED_PATHS` set (which would mask the cross-process contention
//!    we're trying to observe).
//! 2. `flock_holder`: spawn a subprocess that acquires a direct
//!    `flock(2)` on `<dir>/_meta/.lock` and spins. This is what another
//!    process holding the database lock looks like at the kernel level —
//!    bypassing the engine eliminates any subtle interactions with the
//!    holder's own bookkeeping.
//! 3. The parent test invokes `Database::open` / `open_with_options` and
//!    observes the timeout path's behavior.
//!
//! `engine_holder` mirrors the same harness but uses
//! `Database::open`-style holder semantics; it's a sanity check that the
//! lock acquisition through the engine is exactly equivalent to direct
//! `flock(2)` for these tests.

use mongreldb_core::{Database, OpenOptions};
use std::io::BufRead;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn sub_exe() -> PathBuf {
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_cross_process_lock_sub") {
        return PathBuf::from(p);
    }
    let exe = std::env::current_exe().expect("current_exe");
    exe.parent()
        .and_then(|deps| deps.parent())
        .map(|target| target.join("cross_process_lock_sub"))
        .expect("locate cross_process_lock_sub")
}

fn spawn_sub(role: &str, dir: &std::path::Path) -> (std::process::Child, mpsc::Receiver<String>) {
    let mut cmd = Command::new(sub_exe());
    cmd.arg(role).arg(dir);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn sub");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stderr);
        for _line in reader.lines().map_while(Result::ok) {
            // Drain stderr so the child doesn't block on a full stderr pipe.
        }
    });
    (child, rx)
}

fn wait_for_line(rx: &mpsc::Receiver<String>, want: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let remaining = deadline - now;
        match rx.recv_timeout(remaining) {
            Ok(line) if line == want => return true,
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return false,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return false,
        }
    }
}

fn pre_create(dir: &std::path::Path) {
    // Spawn a subprocess that runs `Database::create` and exits. This
    // populates the directory without polluting the parent's per-process
    // `LOCKED_PATHS` set (which would mask the cross-process contention
    // these tests are designed to observe).
    let (mut sub, rx) = spawn_sub("create_then_exit", dir);
    let line = wait_for_line_with(&rx, |l| l == "CREATED", Duration::from_secs(5))
        .expect("create_then_exit");
    assert_eq!(line, "CREATED", "expected CREATED, got: {line:?}");
    let status = sub.wait().expect("wait create sub");
    assert!(status.success(), "create_then_exit failed: {status:?}");
}

fn wait_for_line_with<F: Fn(&str) -> bool>(
    rx: &mpsc::Receiver<String>,
    pred: F,
    timeout: Duration,
) -> Option<String> {
    let deadline = Instant::now() + timeout;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return None;
        }
        let remaining = deadline - now;
        match rx.recv_timeout(remaining) {
            Ok(line) if pred(&line) => return Some(line),
            Ok(_) => continue,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => return None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return None,
        }
    }
}

#[test]
fn losing_concurrent_creator_never_touches_database_directory() {
    let parent = TempDir::new().unwrap();
    let root = parent.path().join("new-database");
    let (mut creator, creator_rx) = spawn_sub("delayed_creator", &root);
    assert!(
        wait_for_line(&creator_rx, "READY", Duration::from_secs(5)),
        "creator never reserved the database lock"
    );

    let error = Database::create(&root).unwrap_err();
    assert!(
        matches!(&error, mongreldb_core::MongrelError::DatabaseLocked { .. }),
        "unexpected creator error: {error}"
    );
    assert!(
        !root.exists(),
        "losing creator wrote the database directory before acquiring its lock"
    );

    assert!(
        wait_for_line(&creator_rx, "CREATED", Duration::from_secs(5)),
        "winning creator never completed"
    );
    assert!(creator.wait().unwrap().success());
    let _db = Database::open(&root).unwrap();
}

#[test]
fn waiting_open_reads_catalog_and_auth_after_lock_acquisition() {
    let dir = TempDir::new().unwrap();
    pre_create(dir.path());
    let (mut enabler, enabler_rx) = spawn_sub("auth_enabler", dir.path());
    assert!(
        wait_for_line(&enabler_rx, "READY", Duration::from_secs(5)),
        "auth enabler never opened database"
    );

    let options = OpenOptions::default().with_lock_timeout_ms(5_000);
    let error = Database::open_with_options(dir.path(), options).unwrap_err();
    assert!(
        matches!(error, mongreldb_core::MongrelError::AuthRequired),
        "waiting open used stale pre-lock catalog: {error}"
    );
    assert!(
        wait_for_line(&enabler_rx, "DONE", Duration::from_secs(5)),
        "auth enabler never completed"
    );
    assert!(enabler.wait().unwrap().success());
    let _db = Database::open_with_credentials(dir.path(), "admin", "admin-password").unwrap();
}

/// Default behavior (timeout=0) must fail fast with a typed lock error.
#[test]
fn fail_fast_default_rejects_concurrent_open() {
    let dir = TempDir::new().unwrap();
    pre_create(dir.path());

    let (mut holder, holder_rx) = spawn_sub("flock_holder", dir.path());
    assert!(
        wait_for_line(&holder_rx, "READY", Duration::from_secs(5)),
        "holder never reported READY"
    );

    let start = Instant::now();
    let result = Database::open(dir.path());
    let elapsed = start.elapsed();

    assert!(
        result.is_err(),
        "default open should fail while another process holds the lock"
    );
    assert!(
        matches!(
            result.unwrap_err(),
            mongreldb_core::MongrelError::DatabaseLocked { .. }
        ),
        "expected DatabaseLocked"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "fail-fast took too long ({elapsed:?}); expected <2s"
    );

    holder.kill().expect("kill holder");
    let _ = holder.wait();
}

/// `lock_timeout_ms=5000` lets the open succeed once the OS lock is
/// released mid-budget. Mirrors SQLite's `busy_timeout` semantics.
#[test]
fn lock_timeout_waits_and_acquires() {
    let dir = TempDir::new().unwrap();
    pre_create(dir.path());

    let (mut holder, holder_rx) = spawn_sub("flock_holder", dir.path());
    assert!(
        wait_for_line(&holder_rx, "READY", Duration::from_secs(5)),
        "holder never READY"
    );

    let dir_path = dir.path().to_path_buf();
    let (done_tx, done_rx) = mpsc::channel::<String>();
    let started = Instant::now();
    let waiter = std::thread::spawn(move || {
        let opts = OpenOptions::default().with_lock_timeout_ms(5000);
        let result = Database::open_with_options(&dir_path, opts);
        let took = started.elapsed();
        match result {
            Ok(_) => done_tx.send(format!("OK after {took:?}")).unwrap(),
            Err(e) => done_tx.send(format!("ERR after {took:?}: {e}")).unwrap(),
        }
    });

    // Hold the lock for ~300ms so the waiter is well into the retry
    // loop, then release and confirm the waiter acquires within budget.
    std::thread::sleep(Duration::from_millis(300));
    holder.kill().expect("kill holder");
    let _ = holder.wait();

    let msg = done_rx
        .recv_timeout(Duration::from_secs(6))
        .expect("waiter did not finish within budget");
    waiter.join().expect("waiter thread panic");
    assert!(msg.starts_with("OK"), "got: {msg}");
}

/// `lock_timeout_ms` that elapses without the lock becoming available
/// returns the timeout-shaped typed lock error.
#[test]
fn lock_timeout_expires_with_error() {
    let dir = TempDir::new().unwrap();
    pre_create(dir.path());

    let (mut holder, holder_rx) = spawn_sub("flock_holder", dir.path());
    assert!(
        wait_for_line(&holder_rx, "READY", Duration::from_secs(5)),
        "holder never READY"
    );

    let dir_path = dir.path().to_path_buf();
    let (done_tx, done_rx) = mpsc::channel::<String>();
    let started = Instant::now();
    let waiter = std::thread::spawn(move || {
        let opts = OpenOptions::default().with_lock_timeout_ms(250);
        let result = Database::open_with_options(&dir_path, opts);
        let took = started.elapsed();
        match result {
            Ok(_) => done_tx
                .send(format!("UNEXPECTED OK after {took:?}"))
                .unwrap(),
            Err(e) => done_tx.send(format!("ERR after {took:?}: {e}")).unwrap(),
        }
    });

    let msg = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("waiter should error within budget");
    waiter.join().expect("waiter thread panic");
    assert!(msg.starts_with("ERR"), "got: {msg}");
    assert!(
        msg.contains("is locked") && msg.contains("250ms"),
        "expected timeout-shaped error, got: {msg}"
    );

    holder.kill().expect("kill holder");
    let _ = holder.wait();
}

/// `Database::open`'s own lock acquisition, when invoked across
/// processes via the engine path, also observes the timeout knob. This
/// is the cross-process end-to-end test against the holder that goes
/// through `Database::open` itself (no direct `flock`).
#[test]
fn engine_holder_blocks_in_process_open() {
    let dir = TempDir::new().unwrap();
    pre_create(dir.path());

    let (mut holder, holder_rx) = spawn_sub("engine_holder", dir.path());
    assert!(
        wait_for_line(&holder_rx, "READY", Duration::from_secs(5)),
        "engine_holder never READY"
    );

    let result = Database::open(dir.path());
    assert!(
        result.is_err(),
        "default open should fail while subprocess holds the lock"
    );
    assert!(
        matches!(
            result.unwrap_err(),
            mongreldb_core::MongrelError::DatabaseLocked { .. }
        ),
        "expected DatabaseLocked"
    );

    holder.kill().expect("kill holder");
    let _ = holder.wait();
}

/// End-to-end worked example of the new `OpenOptions::lock_timeout_ms` knob:
/// 4 real subprocesses each open the same DB, each commits 25 transactions
/// in a tight loop. Without the timeout knob this would hit EAGAIN as soon
/// as a second writer tried to open while another held the lock; with
/// `lock_timeout_ms = 5_000` every writer waits its turn, all 4 succeed,
/// and the row count matches.
///
/// Regression guard: if the new `lock_timeout_ms` knob regresses to fail-fast
/// (or the lock acquisition gets dropped entirely), this test fails loudly.
#[test]
fn cross_process_writers_all_succeed() {
    const N_WRITERS: i64 = 4;
    const ROWS_PER_WRITER: i64 = 25;
    const LOCK_TIMEOUT_MS: u32 = 5_000;

    let dir = TempDir::new().unwrap();
    pre_create(dir.path());

    // Each writer is its own OS process; they race on the cross-process
    // lock from the moment they try to open. With the new timeout knob
    // they should all complete cleanly.
    let sub_path = std::env::var("CARGO_BIN_EXE_cross_process_lock_sub")
        .expect("CARGO_BIN_EXE_cross_process_lock_sub must be set by cargo test");
    let mut handles = Vec::new();
    for writer_id in 0..N_WRITERS {
        let mut cmd = std::process::Command::new(&sub_path);
        cmd.arg("writer")
            .arg(dir.path())
            .arg(writer_id.to_string())
            .arg(LOCK_TIMEOUT_MS.to_string())
            .arg(ROWS_PER_WRITER.to_string())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().expect("spawn writer");
        let stdout = child.stdout.take().expect("writer stdout");
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        std::thread::spawn(move || {
            use std::io::{BufRead, BufReader};
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    break;
                }
            }
        });
        handles.push((child, rx));
    }

    // Wait for all writers to report DONE <id>. Failures (e.g. lock timeout
    // expiry, fsync errors) cause a non-zero exit which we surface via
    // `child.wait()` below.
    let mut completed: Vec<i64> = Vec::new();
    let mut failed: Vec<(i64, String, Option<i32>)> = Vec::new();
    for (mut child, rx) in handles {
        let writer_id_line =
            wait_for_line_with(&rx, |l| l.starts_with("DONE"), Duration::from_secs(30))
                .unwrap_or_else(|| {
                    failed.push((-1, "writer never reported DONE".to_string(), None));
                    String::new()
                });
        let status = child.wait().expect("wait writer");
        if !status.success() {
            failed.push((
                -1,
                format!("writer exited non-zero: {status}"),
                status.code(),
            ));
            continue;
        }
        // Parse "DONE <writer_id>" — the writer_id was passed positionally
        // so we trust the order of completion matches the order of spawn.
        let parsed_id = writer_id_line
            .strip_prefix("DONE ")
            .and_then(|s| s.parse::<i64>().ok());
        if let Some(id) = parsed_id {
            completed.push(id);
        } else {
            failed.push((-1, format!("malformed DONE line: {writer_id_line:?}"), None));
        }
    }

    assert!(
        failed.is_empty(),
        "writers failed: {failed:?}; expected all {} to succeed with lock_timeout_ms={LOCK_TIMEOUT_MS}",
        N_WRITERS
    );
    assert_eq!(
        completed.len() as i64,
        N_WRITERS,
        "expected {N_WRITERS} writers to complete, got {completed:?}"
    );

    // Verify the row count: every writer committed ROWS_PER_WRITER rows.
    // We open the parent in this process to count — this only works because
    // all writers have exited and released their locks.
    let db = Database::open(dir.path()).expect("parent open after writers done");
    let table = db.table("items").expect("items table");
    let count = table.lock().count();
    assert_eq!(
        count,
        (N_WRITERS * ROWS_PER_WRITER) as u64,
        "expected {} rows total, got {count}",
        N_WRITERS * ROWS_PER_WRITER
    );
}
