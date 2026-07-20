//! True process-level crash harness: spawns the `crash_writer` bin, waits for it
//! to reach a known pre-crash state, delivers `SIGKILL` (`kill -9`), then reopens
//! the DB and asserts the recovery invariant. Unlike the simulated crash tests
//! in `crash.rs` (which manipulate on-disk files directly), these tests kill a
//! *real* child process mid-run — exercising the actual OS-level crash path
//! (kernel reaping the process, unclosed file handles, page-cache state).
//!
//! On Unix, `Child::kill` is documented to send `SIGKILL`, i.e. `kill -9`.

use mongreldb_core::query::{Retriever, RetrieverScore};
use mongreldb_core::schema::AnnQuantization;
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
    spawn_and_kill_args(&[mode.to_string()], ready, db_dir);
}

/// `spawn_and_kill` with extra arguments forwarded to `crash_writer`.
fn spawn_and_kill_args(args: &[String], ready: &str, db_dir: &std::path::Path) {
    let mut child = Command::new(writer_exe())
        .arg(db_dir)
        .args(args)
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

/// Repeated unclean sessions against the shared (mounted-table) WAL — the
/// configuration multi-table databases run in production, where every process
/// stop is an unclean shutdown. Each cycle commits one row in a fresh child,
/// delivers SIGKILL, then reopens: every acknowledged row must be present,
/// the database must pass integrity checks, and the next write must succeed.
/// Finally the retained WAL history must keep one globally contiguous record
/// sequence across all of those crashed sessions.
#[test]
fn repeated_kill9_cycles_preserve_rows_and_wal_sequence() {
    use mongreldb_core::SharedWal;

    let dir = TempDir::new().unwrap();
    const CYCLES: i64 = 20;
    for cycle in 1..=CYCLES {
        spawn_and_kill_args(
            &["shared-commit".into(), cycle.to_string()],
            "SHARED_READY",
            dir.path(),
        );

        let db = Database::open(dir.path()).expect("reopen after kill -9");
        assert_eq!(
            db.table("t").unwrap().lock().count() as i64,
            cycle,
            "every committed row survives cycle {cycle}"
        );
        assert!(
            db.check().is_empty(),
            "integrity check passes after cycle {cycle}"
        );
        drop(db);
    }

    let records = SharedWal::replay(dir.path()).expect("replay shared WAL");
    let sequences: Vec<u64> = records.iter().map(|record| record.seq.0).collect();
    assert!(
        sequences.windows(2).all(|pair| pair[1] == pair[0] + 1),
        "WAL sequence stays globally contiguous across crashed sessions"
    );
    assert!(
        sequences.len() >= CYCLES as usize * 2,
        "every cycle's commit records are retained or checkpointed"
    );
}

/// The encrypted matrix of the restart cycle: encryption must not change
/// unclean-shutdown recovery semantics.
#[test]
fn repeated_kill9_cycles_preserve_rows_and_wal_sequence_encrypted() {
    let dir = TempDir::new().unwrap();
    const CYCLES: i64 = 3;
    for cycle in 1..=CYCLES {
        spawn_and_kill_args(
            &["shared-commit-encrypted".into(), cycle.to_string()],
            "SHARED_READY",
            dir.path(),
        );

        let db = Database::open_encrypted(dir.path(), "crash-harness-test-passphrase")
            .expect("reopen encrypted database after kill -9");
        assert_eq!(db.table("t").unwrap().lock().count() as i64, cycle);
        assert!(db.check().is_empty());
        drop(db);
    }
}

#[test]
fn every_canonical_durable_hook_survives_real_kill9() {
    const HOOKS: &[&str] = &[
        "wal.append.before",
        "wal.append.after",
        "wal.fsync.before",
        "wal.fsync.after",
        "commit.publish.before",
        "commit.publish.after",
        "catalog.publish.before",
        "catalog.publish.after",
        "snapshot.install.before",
        "snapshot.install.after",
        "index.publish.before",
        "index.publish.after",
    ];

    for hook in HOOKS {
        let dir = TempDir::new().unwrap();
        spawn_and_kill_args(
            &["durable-hook".into(), (*hook).into()],
            "HOOK_READY",
            dir.path(),
        );
        match *hook {
            "snapshot.install.before" | "snapshot.install.after" => {
                let leader = Database::open(dir.path().join("leader"))
                    .unwrap_or_else(|error| panic!("{hook}: reopen leader: {error}"));
                let follower_path = dir.path().join("follower");
                if !follower_path.exists() {
                    leader
                        .replication_snapshot()
                        .unwrap()
                        .install(&follower_path)
                        .unwrap_or_else(|error| panic!("{hook}: retry snapshot install: {error}"));
                }
                let follower = Database::open(&follower_path)
                    .unwrap_or_else(|error| panic!("{hook}: reopen follower: {error}"));
                assert!(follower.is_read_only_replica(), "{hook}");
                assert_eq!(follower.table("items").unwrap().lock().count(), 1, "{hook}");
            }
            "index.publish.before" | "index.publish.after" => {
                let db = Database::open(dir.path())
                    .unwrap_or_else(|error| panic!("{hook}: reopen database: {error}"));
                assert_eq!(db.table("items").unwrap().lock().count(), 1, "{hook}");
                assert!(db.check().is_empty(), "{hook}");
            }
            "catalog.publish.before" | "catalog.publish.after" => {
                let db = Database::open(dir.path())
                    .unwrap_or_else(|error| panic!("{hook}: reopen database: {error}"));
                assert!(db.table("published").is_ok(), "{hook}");
                assert!(db.check().is_empty(), "{hook}");
            }
            _ => {
                let db = Database::open(dir.path())
                    .unwrap_or_else(|error| panic!("{hook}: reopen database: {error}"));
                let count = db.table("items").unwrap().lock().count();
                assert!((1..=2).contains(&count), "{hook}: row count {count}");
                assert!(db.check().is_empty(), "{hook}");
                if matches!(*hook, "commit.publish.before" | "commit.publish.after") {
                    assert_eq!(count, 2, "{hook}: durable row was lost");
                }
            }
        }
    }
}

#[test]
fn online_index_publication_hooks_recover_complete_authoritative_state_after_kill9() {
    for hook in ["index.publish.before", "index.publish.after"] {
        let dir = TempDir::new().unwrap();
        spawn_and_kill_args(
            &["index-ddl-hook".into(), hook.into()],
            "HOOK_READY",
            dir.path(),
        );
        let db = Database::open(dir.path())
            .unwrap_or_else(|error| panic!("{hook}: reopen database: {error}"));
        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        let index = table
            .schema()
            .indexes
            .iter()
            .find(|index| index.name == "idx_embedding");
        if hook == "index.publish.before" {
            assert!(index.is_none(), "before-boundary crash published schema");
        } else {
            assert_eq!(
                index
                    .and_then(|index| index.options.ann.as_ref())
                    .map(|options| options.quantization),
                Some(AnnQuantization::Dense),
                "after-boundary crash lost Dense schema"
            );
            let hits = table
                .retrieve(&Retriever::Ann {
                    column_id: 2,
                    query: vec![1.0, 0.0, 0.0, 0.0],
                    k: 1,
                })
                .unwrap();
            assert_eq!(hits.len(), 1);
            assert!(matches!(
                hits[0].score,
                RetrieverScore::AnnCosineDistance(distance) if distance.abs() < 1e-5
            ));
        }
        assert_eq!(table.count(), 1);
        drop(table);
        assert!(db.check().is_empty(), "{hook}");
    }
}
