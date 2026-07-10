//! mongreldb-server entry point.
//!
//! Supports `--daemon` mode (fork into background), graceful signal handling
//! (SIGINT/SIGTERM flush all tables then exit), a proper flag parser, and
//! subcommands for deterministic-stable snapshots:
//!
//!   mongreldb-server snapshot <db_dir>   — checkpoint to a stable byte image
//!   mongreldb-server restore  <db_dir>   — open + verify + checkpoint

use mongreldb_core::Database;
use mongreldb_server::{build_app_full, spawn_auto_compactor};
use std::net::SocketAddr;
use std::sync::Arc;

/// Parsed command-line arguments.
struct Args {
    db_dir: String,
    port: u16,
    auth_token: Option<String>,
    user_auth: bool,
    max_connections: Option<usize>,
    passphrase: Option<String>,
    daemon: bool,
    pidfile: Option<String>,
}

const DEFAULT_PORT: u16 = 8453;

const USAGE: &str = "\
mongreldb-server — HTTP daemon for MongrelDB

USAGE:
    mongreldb-server <db_dir> [options]

ARGS:
    <db_dir>            Database directory (required, first positional arg)
    <port>              Optional second positional arg (numeric) for backward compat

OPTIONS:
    --port <port>               Listen port (default 8453)
    --auth-token <token>        Enable Bearer token authentication
    --auth-users                Enable Basic user authentication
    --max-connections <n>       Max concurrent connections
    --passphrase <passphrase>   Open an encrypted database
    --daemon                    Fork into the background (daemonize)
    --pidfile <path>            PID file path (default: <db_dir>/mongreldb.pid)
    -h, --help                  Print this help message
";

/// Parse command-line arguments. Returns `Err(message)` on failure.
fn parse_args() -> Result<Args, String> {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() < 2 {
        return Err(format!(
            "error: a database directory is required\n\n{USAGE}"
        ));
    }

    let mut db_dir: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut auth_token: Option<String> = None;
    let mut user_auth = false;
    let mut max_connections: Option<usize> = None;
    let mut passphrase: Option<String> = None;
    let mut daemon = false;
    let mut pidfile: Option<String> = None;

    // Skip the program name (raw[0]).
    let mut i = 1;
    while i < raw.len() {
        let arg = &raw[i];
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            "--port" => {
                let v = raw.get(i + 1).ok_or("--port requires a value")?;
                port = Some(
                    v.parse::<u16>()
                        .map_err(|_| format!("--port: invalid port '{v}'"))?,
                );
                i += 2;
            }
            "--auth-token" => {
                let v = raw.get(i + 1).ok_or("--auth-token requires a value")?;
                auth_token = Some(v.clone());
                i += 2;
            }
            "--auth-users" => {
                user_auth = true;
                i += 1;
            }
            "--max-connections" => {
                let v = raw.get(i + 1).ok_or("--max-connections requires a value")?;
                max_connections = Some(
                    v.parse::<usize>()
                        .map_err(|_| format!("--max-connections: invalid value '{v}'"))?,
                );
                i += 2;
            }
            "--passphrase" => {
                let v = raw.get(i + 1).ok_or("--passphrase requires a value")?;
                passphrase = Some(v.clone());
                i += 2;
            }
            "--daemon" => {
                daemon = true;
                i += 1;
            }
            "--pidfile" => {
                let v = raw.get(i + 1).ok_or("--pidfile requires a value")?;
                pidfile = Some(v.clone());
                i += 2;
            }
            // Positional: first is db_dir, second (if numeric) is port for backward compat.
            other => {
                if db_dir.is_none() {
                    db_dir = Some(other.to_string());
                } else if port.is_none() {
                    // Treat as backward-compat positional port only if numeric.
                    if let Ok(p) = other.parse::<u16>() {
                        port = Some(p);
                    } else {
                        return Err(format!("unexpected argument: {other}\n\n{USAGE}"));
                    }
                } else {
                    return Err(format!("unexpected argument: {other}\n\n{USAGE}"));
                }
                i += 1;
            }
        }
    }

    let db_dir = db_dir.ok_or_else(|| format!("a database directory is required\n\n{USAGE}"))?;
    let port = port.unwrap_or(DEFAULT_PORT);

    Ok(Args {
        db_dir,
        port,
        auth_token,
        user_auth,
        max_connections,
        passphrase,
        daemon,
        pidfile,
    })
}

/// Resolve the pidfile path: explicit `--pidfile`, else `<db_dir>/mongreldb.pid`.
fn resolve_pidfile(args: &Args) -> String {
    if let Some(ref p) = args.pidfile {
        p.clone()
    } else {
        let mut p = std::path::PathBuf::from(&args.db_dir);
        p.push("mongreldb.pid");
        p.to_string_lossy().into_owned()
    }
}

/// Fork into the background (classic double-fork-style daemonization using
/// `setsid`). The parent writes the child PID to `pidfile` and exits 0.
fn daemonize(pidfile: &str) -> Result<(), String> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fork()` has no preconditions. We check the return value.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err("fork() failed".to_string());
    }
    if pid > 0 {
        // Parent: write the child PID and exit immediately.
        if let Err(e) = std::fs::write(pidfile, format!("{pid}\n")) {
            eprintln!("warning: could not write pidfile {pidfile}: {e}");
        }
        std::process::exit(0);
    }

    // Child: become a new session leader to detach from the controlling terminal.
    // SAFETY: `setsid()` has no preconditions.
    if unsafe { libc::setsid() } < 0 {
        return Err("setsid() failed".to_string());
    }

    // Redirect stdin/stdout/stderr to /dev/null.
    let devnull = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
        .map_err(|e| format!("open /dev/null: {e}"))?;
    let fd = devnull.as_raw_fd();
    // SAFETY: `dup2` just duplicates an open fd onto the standard streams.
    unsafe {
        libc::dup2(fd, libc::STDIN_FILENO);
        libc::dup2(fd, libc::STDOUT_FILENO);
        libc::dup2(fd, libc::STDERR_FILENO);
    }
    // Keep `devnull` alive for the rest of the process by forgetting it.
    std::mem::forget(devnull);

    Ok(())
}

#[tokio::main]
async fn main() {
    // ── Subcommand dispatch ─────────────────────────────────────────────────
    //
    // `snapshot` and `restore` are one-shot maintenance subcommands that
    // operate on the database directory and exit (they do NOT start the HTTP
    // server). Everything else is the daemon mode.
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 2 {
        match raw[1].as_str() {
            "snapshot" => {
                let db_dir = raw.get(2).cloned().unwrap_or_else(|| {
                    eprintln!("usage: mongreldb-server snapshot <db_dir>");
                    std::process::exit(1);
                });
                cmd_snapshot(&db_dir);
                return;
            }
            "restore" => {
                let db_dir = raw.get(2).cloned().unwrap_or_else(|| {
                    eprintln!("usage: mongreldb-server restore <db_dir>");
                    std::process::exit(1);
                });
                cmd_restore(&db_dir);
                return;
            }
            _ => {}
        }
    }

    // ── Daemon mode ─────────────────────────────────────────────────────────
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let pidfile = resolve_pidfile(&args);

    if args.daemon {
        if let Err(e) = daemonize(&pidfile) {
            eprintln!("daemonize failed: {e}");
            std::process::exit(1);
        }
    }

    // Open the database (optionally encrypted), or create it if the catalog
    // is absent. CATALOG existence is now also enforced by `create` itself,
    // but we branch here so the error path on a populated dir is `open`'s
    // (more specific) error, not the generic "database already exists" from
    // `create`.
    let catalog_exists = std::path::Path::new(&args.db_dir).join("CATALOG").exists();
    let db = if let Some(ref pw) = args.passphrase {
        Arc::new(
            Database::open_encrypted(&args.db_dir, pw).unwrap_or_else(|e| {
                eprintln!("failed to open {}: {e}", args.db_dir);
                std::process::exit(1);
            }),
        )
    } else if catalog_exists {
        Arc::new(Database::open(&args.db_dir).unwrap_or_else(|e| {
            eprintln!("failed to open {}: {e}", args.db_dir);
            std::process::exit(1);
        }))
    } else {
        Arc::new(Database::create(&args.db_dir).unwrap_or_else(|e| {
            eprintln!("failed to create {}: {e}", args.db_dir);
            std::process::exit(1);
        }))
    };

    // §5.9: background cost-aware compaction (run-count trigger).
    spawn_auto_compactor(Arc::clone(&db));

    let app = build_app_full(
        db.clone(),
        std::iter::empty(),
        args.auth_token.clone(),
        args.max_connections,
        args.user_auth,
    );

    if args.auth_token.is_some() {
        eprintln!("token authentication enabled (Authorization: Bearer <token>)");
    }
    if args.user_auth {
        eprintln!("user authentication enabled (Authorization: Basic <user:pass>)");
    }
    if let Some(max) = args.max_connections {
        eprintln!("connection limit: {max}");
    }

    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    eprintln!("mongreldb-server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {addr}: {e}");
            std::process::exit(1);
        });

    // Graceful shutdown via tokio::select!. Race the server against SIGINT
    // (ctrl_c) and SIGTERM (unix). Whichever fires first wins; we then flush
    // all tables via `db.close()` before exiting.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            result = axum::serve(listener, app) => {
                if let Err(e) = result {
                    eprintln!("server error: {e}");
                    shutdown(&db, &pidfile, args.daemon);
                    std::process::exit(1);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received SIGINT, shutting down gracefully...");
            }
            _ = sigterm.recv() => {
                eprintln!("received SIGTERM, shutting down gracefully...");
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = axum::serve(listener, app) => {
                if let Err(e) = result {
                    eprintln!("server error: {e}");
                    shutdown(&db, &pidfile, args.daemon);
                    std::process::exit(1);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received SIGINT, shutting down gracefully...");
            }
        }
    }

    shutdown(&db, &pidfile, args.daemon);
}

/// Flush all tables, checkpoint to a stable on-disk state, and remove the
/// pidfile (if we wrote one). The checkpoint ensures the database directory
/// is deterministic after shutdown — no stale WAL segments, no fragmented
/// runs — so `git status` shows clean when the directory is tracked.
fn shutdown(db: &Arc<Database>, pidfile: &str, daemon: bool) {
    // Checkpoint: flush + compact + reap WAL segments + rotate active segment.
    // This normalizes the on-disk state to a deterministic form.
    match db.checkpoint() {
        Ok(()) => eprintln!("checkpoint complete"),
        Err(e) => {
            // Checkpoint failure is non-fatal during shutdown — fall back to
            // a best-effort close so the process can still exit cleanly.
            eprintln!("checkpoint failed (falling back to close): {e}");
            let _ = db.close();
        }
    }
    eprintln!("shutdown complete");
    if daemon {
        let _ = std::fs::remove_file(pidfile);
    }
}

// ── Subcommand handlers ─────────────────────────────────────────────────────

/// `mongreldb-server snapshot <db_dir>`
///
/// Produce a deterministic-stable byte image: flush all writes, compact all
/// tables, reap all WAL segments, rotate to a fresh empty segment. The
/// resulting directory can be safely `git add`-ed and `git checkout`-ed
/// without stale WAL tail bytes or segment count drift.
fn cmd_snapshot(db_dir: &str) {
    let db = match Database::open(db_dir).or_else(|_| Database::create(db_dir)) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: cannot open {}: {e}", db_dir);
            std::process::exit(1);
        }
    };
    match db.checkpoint() {
        Ok(()) => {
            println!("snapshot stable at {}", db_dir);
        }
        Err(e) => {
            eprintln!("error: checkpoint failed: {e}");
            std::process::exit(1);
        }
    }
}

/// `mongreldb-server restore <db_dir>`
///
/// Open the database (replaying any remaining WAL), verify integrity, then
/// checkpoint to a stable state. Use this after `git checkout` to ensure the
/// directory is in a consistent, deterministic state before use.
fn cmd_restore(db_dir: &str) {
    let db = match Database::open(db_dir).or_else(|_| Database::create(db_dir)) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: cannot open {}: {e}", db_dir);
            std::process::exit(1);
        }
    };

    // Verify integrity (check for issues like torn writes, checksum mismatches).
    let issues = db.check();
    if !issues.is_empty() {
        eprintln!("warning: {} integrity issue(s) found:", issues.len());
        for issue in &issues {
            eprintln!("  - {:?}", issue);
        }
    }

    match db.checkpoint() {
        Ok(()) => {
            println!("restored and checkpointed at {}", db_dir);
        }
        Err(e) => {
            eprintln!("error: checkpoint failed: {e}");
            std::process::exit(1);
        }
    }
}
