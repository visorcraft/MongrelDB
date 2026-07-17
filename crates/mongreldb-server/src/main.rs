//! mongreldb-server entry point.
//!
//! Supports `--daemon` mode (fork into background), graceful signal handling
//! (SIGINT/SIGTERM flush all tables then exit; SIGHUP reloads the mutable
//! configuration subset live, spec §10.7), a proper flag parser, and
//! subcommands for deterministic-stable snapshots:
//!
//!   mongreldb-server snapshot <db_dir>   — checkpoint to a stable byte image
//!   mongreldb-server restore  <db_dir>   — open + verify + checkpoint

use mongreldb_core::Database;
use mongreldb_server::{
    build_app_with_sessions_and_control, spawn_auto_compactor, spawn_session_reaper, SessionStore,
};
use std::ffi::OsString;
use std::net::SocketAddr;
use std::sync::Arc;
use zeroize::Zeroizing;

/// Parsed command-line arguments.
struct Args {
    db_dir: String,
    port: u16,
    auth_token: Option<String>,
    user_auth: bool,
    max_connections: Option<usize>,
    max_sessions: usize,
    session_idle_timeout_secs: u64,
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
    --max-sessions <n>          Max live sessions for cross-request txns (default 256)
    --session-idle-timeout <s>  Idle session reaping timeout in seconds (default 300)
    --passphrase <passphrase>   Open an encrypted database
    --daemon                    Fork into the background (daemonize)
    --pidfile <path>            PID file path (default: <db_dir>/mongreldb.pid)
    -h, --help                  Print this help message

ENVIRONMENT:
    MONGRELDB_DB_USERNAME       Database-handle username (set with DB_PASSWORD)
    MONGRELDB_DB_PASSWORD       Database-handle password (set with DB_USERNAME)
";

struct DatabaseCredentials {
    username: String,
    password: Zeroizing<String>,
}

fn database_credentials_from_values(
    username: Option<OsString>,
    password: Option<OsString>,
) -> Result<Option<DatabaseCredentials>, String> {
    let (username, password) = match (username, password) {
        (None, None) => return Ok(None),
        (Some(username), Some(password)) => (username, password),
        _ => {
            return Err(
                "MONGRELDB_DB_USERNAME and MONGRELDB_DB_PASSWORD must be set together".into(),
            )
        }
    };
    let username = username
        .into_string()
        .map_err(|_| "MONGRELDB_DB_USERNAME must be valid UTF-8".to_string())?;
    let password = Zeroizing::new(
        password
            .into_string()
            .map_err(|_| "MONGRELDB_DB_PASSWORD must be valid UTF-8".to_string())?,
    );
    if username.is_empty() {
        return Err("MONGRELDB_DB_USERNAME must not be empty".into());
    }
    if password.is_empty() {
        return Err("MONGRELDB_DB_PASSWORD must not be empty".into());
    }
    Ok(Some(DatabaseCredentials { username, password }))
}

/// Read database-open credentials once, then remove them before daemonization
/// or worker threads can inherit the environment. The password remains in a
/// `Zeroizing<String>` only until the database open/create call returns.
fn take_database_credentials_from_env() -> Result<Option<DatabaseCredentials>, String> {
    let username = std::env::var_os("MONGRELDB_DB_USERNAME");
    let password = std::env::var_os("MONGRELDB_DB_PASSWORD");
    std::env::remove_var("MONGRELDB_DB_USERNAME");
    std::env::remove_var("MONGRELDB_DB_PASSWORD");
    database_credentials_from_values(username, password)
}

fn open_or_create_database(
    db_dir: &str,
    passphrase: Option<&str>,
    credentials: Option<DatabaseCredentials>,
) -> mongreldb_core::Result<Database> {
    let catalog_exists = std::path::Path::new(db_dir).join("CATALOG").exists();
    match (catalog_exists, passphrase, credentials.as_ref()) {
        (true, Some(passphrase), Some(credentials)) => Database::open_encrypted_with_credentials(
            db_dir,
            passphrase,
            &credentials.username,
            credentials.password.as_str(),
        ),
        (false, Some(passphrase), Some(credentials)) => {
            Database::create_encrypted_with_credentials(
                db_dir,
                passphrase,
                &credentials.username,
                credentials.password.as_str(),
            )
        }
        (true, None, Some(credentials)) => Database::open_with_credentials(
            db_dir,
            &credentials.username,
            credentials.password.as_str(),
        ),
        (false, None, Some(credentials)) => Database::create_with_credentials(
            db_dir,
            &credentials.username,
            credentials.password.as_str(),
        ),
        (true, Some(passphrase), None) => Database::open_encrypted(db_dir, passphrase),
        (false, Some(passphrase), None) => Database::create_encrypted(db_dir, passphrase),
        (true, None, None) => Database::open(db_dir),
        (false, None, None) => Database::create(db_dir),
    }
}

fn validate_http_auth_configuration(
    db: &Database,
    auth_token: Option<&str>,
    user_auth: bool,
) -> Result<(), String> {
    if db.require_auth_enabled() && auth_token.is_none() && !user_auth {
        return Err(
            "this database requires authentication; configure --auth-users or --auth-token".into(),
        );
    }
    Ok(())
}

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
    let mut max_sessions: usize = 256;
    let mut session_idle_timeout_secs: u64 = 300;
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
            "--max-sessions" => {
                let v = raw.get(i + 1).ok_or("--max-sessions requires a value")?;
                max_sessions = v
                    .parse::<usize>()
                    .map_err(|_| format!("--max-sessions: invalid value '{v}'"))?;
                i += 2;
            }
            "--session-idle-timeout" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--session-idle-timeout requires a value")?;
                session_idle_timeout_secs = v
                    .parse::<u64>()
                    .map_err(|_| format!("--session-idle-timeout: invalid value '{v}'"))?;
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
        max_sessions,
        session_idle_timeout_secs,
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

fn main() {
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
    let database_credentials = match take_database_credentials_from_env() {
        Ok(credentials) => credentials,
        Err(error) => {
            eprintln!("database credentials: {error}");
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

    // Credential ownership is moved into this call. Its password is zeroized
    // immediately after open/create returns, before any worker thread starts.
    let db = Arc::new(
        open_or_create_database(
            &args.db_dir,
            args.passphrase.as_deref(),
            database_credentials,
        )
        .unwrap_or_else(|e| {
            eprintln!("failed to open or create {}: {e}", args.db_dir);
            std::process::exit(1);
        }),
    );
    if let Err(error) =
        validate_http_auth_configuration(&db, args.auth_token.as_deref(), args.user_auth)
    {
        eprintln!("failed to start: {error}");
        std::process::exit(1);
    }

    // Build Tokio only after the credential environment is cleared and the
    // password-owning `Zeroizing<String>` has been dropped by database open.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| {
            eprintln!("failed to start async runtime: {error}");
            let _ = db.close();
            std::process::exit(1);
        });
    runtime.block_on(run_server(args, pidfile, db));
}

async fn run_server(args: Args, pidfile: String, db: Arc<Database>) {
    // §5.9: background cost-aware compaction (run-count trigger).
    spawn_auto_compactor(Arc::clone(&db));

    // Cross-request session store for interactive transactions. The reaper
    // shares this Arc so it sweeps the same map the handlers use.
    let sessions = Arc::new(SessionStore::new(
        args.max_sessions,
        std::time::Duration::from_secs(args.session_idle_timeout_secs),
    ));
    spawn_session_reaper(Arc::clone(&sessions));

    let (app, server_control) = build_app_with_sessions_and_control(
        db.clone(),
        std::iter::empty(),
        args.auth_token.clone(),
        args.max_connections,
        args.user_auth,
        sessions,
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
    eprintln!(
        "sessions: max {} (idle timeout {}s) \u{2014} cross-request txns via X-Session-ID",
        args.max_sessions, args.session_idle_timeout_secs
    );

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
    // all tables via `db.close()` before exiting. SIGHUP instead triggers a
    // live reload of the mutable configuration subset (§10.7) via a dedicated
    // task so the signal never terminates the serve loop.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
        let reload_control = server_control.clone();
        tokio::spawn(async move {
            while sighup.recv().await.is_some() {
                match reload_control.reload_config() {
                    Ok(report) => eprintln!(
                        "[config] SIGHUP: mutable configuration reloaded: {}",
                        serde_json::to_string(&report)
                            .unwrap_or_else(|_| "<serialize error>".to_string())
                    ),
                    Err(error) => eprintln!("[config] SIGHUP reload failed: {error}"),
                }
            }
        });
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

    let stuck_queries = server_control.shutdown().await;
    if stuck_queries > 0 {
        eprintln!("[shutdown] {stuck_queries} SQL query(s) exceeded cancellation grace");
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENVIRONMENT_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn credentials(username: &str, password: &str) -> DatabaseCredentials {
        database_credentials_from_values(
            Some(OsString::from(username)),
            Some(OsString::from(password)),
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    fn database_credentials_must_be_paired_nonempty_utf8() {
        assert!(database_credentials_from_values(Some(OsString::from("admin")), None).is_err());
        assert!(database_credentials_from_values(None, Some(OsString::from("password"))).is_err());
        assert!(database_credentials_from_values(
            Some(OsString::from("")),
            Some(OsString::from("password"))
        )
        .is_err());
        assert!(database_credentials_from_values(
            Some(OsString::from("admin")),
            Some(OsString::from(""))
        )
        .is_err());
    }

    #[test]
    fn database_credentials_are_removed_from_the_environment() {
        let _guard = ENVIRONMENT_TEST_LOCK.lock().unwrap();
        std::env::set_var("MONGRELDB_DB_USERNAME", "admin");
        std::env::set_var("MONGRELDB_DB_PASSWORD", "database-password");

        let credentials = take_database_credentials_from_env().unwrap().unwrap();

        assert_eq!(credentials.username, "admin");
        assert_eq!(credentials.password.as_str(), "database-password");
        assert!(std::env::var_os("MONGRELDB_DB_USERNAME").is_none());
        assert!(std::env::var_os("MONGRELDB_DB_PASSWORD").is_none());
    }

    #[test]
    fn credentialed_plain_database_create_reopen_and_http_auth_validation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let database =
            open_or_create_database(path, None, Some(credentials("admin", "database-password")))
                .unwrap();
        assert!(database.require_auth_enabled());
        assert_eq!(
            database.principal_snapshot().unwrap().username,
            "admin".to_string()
        );
        assert!(validate_http_auth_configuration(&database, None, false).is_err());
        assert!(validate_http_auth_configuration(&database, Some("token"), false).is_ok());
        assert!(validate_http_auth_configuration(&database, None, true).is_ok());
        drop(database);

        let reopened =
            open_or_create_database(path, None, Some(credentials("admin", "database-password")))
                .unwrap();
        assert_eq!(reopened.principal_snapshot().unwrap().username, "admin");
        drop(reopened);

        assert!(
            open_or_create_database(path, None, Some(credentials("admin", "wrong-password")),)
                .is_err()
        );
    }

    #[test]
    fn credentialed_encrypted_database_create_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let database = open_or_create_database(
            path,
            Some("encryption-passphrase"),
            Some(credentials("admin", "database-password")),
        )
        .unwrap();
        assert!(database.require_auth_enabled());
        drop(database);

        let reopened = open_or_create_database(
            path,
            Some("encryption-passphrase"),
            Some(credentials("admin", "database-password")),
        )
        .unwrap();
        assert_eq!(reopened.principal_snapshot().unwrap().username, "admin");
    }
}
