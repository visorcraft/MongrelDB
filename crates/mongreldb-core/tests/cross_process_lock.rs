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
        for line in reader.lines().flatten() {
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stderr);
        for _line in reader.lines().flatten() {
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

/// Default behavior (timeout=0) must fail fast with the existing
/// `locked by another process` error so every caller that doesn't opt
/// in keeps working unchanged.
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
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("locked by another process"),
        "expected 'locked by another process' error, got: {err}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "fail-fast took too long ({elapsed:?}); expected <500ms"
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
/// returns the timeout-shaped error mapped through `MongrelError::Io`.
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
            Ok(_) => done_tx.send(format!("UNEXPECTED OK after {took:?}")).unwrap(),
            Err(e) => done_tx.send(format!("ERR after {took:?}: {e}")).unwrap(),
        }
    });

    let msg = done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("waiter should error within budget");
    waiter.join().expect("waiter thread panic");
    assert!(msg.starts_with("ERR"), "got: {msg}");
    assert!(
        msg.contains("locked by another process") && msg.contains("250ms"),
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
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("locked by another process"),
        "expected 'locked by another process' error, got: {err}"
    );

    holder.kill().expect("kill holder");
    let _ = holder.wait();
}
