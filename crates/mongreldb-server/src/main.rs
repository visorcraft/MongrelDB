//! mongreldb-server entry point.
//!
//! Supports `--daemon` mode (fork into background), graceful signal handling
//! (SIGINT/SIGTERM flush all tables then exit), and a proper flag parser.

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
                let v = raw
                    .get(i + 1)
                    .ok_or("--port requires a value")?;
                port = Some(
                    v.parse::<u16>()
                        .map_err(|_| format!("--port: invalid port '{v}'"))?,
                );
                i += 2;
            }
            "--auth-token" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--auth-token requires a value")?;
                auth_token = Some(v.clone());
                i += 2;
            }
            "--auth-users" => {
                user_auth = true;
                i += 1;
            }
            "--max-connections" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--max-connections requires a value")?;
                max_connections = Some(
                    v.parse::<usize>()
                        .map_err(|_| format!("--max-connections: invalid value '{v}'"))?,
                );
                i += 2;
            }
            "--passphrase" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--passphrase requires a value")?;
                passphrase = Some(v.clone());
                i += 2;
            }
            "--daemon" => {
                daemon = true;
                i += 1;
            }
            "--pidfile" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--pidfile requires a value")?;
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

    // Open the database (optionally encrypted). If the catalog doesn't exist
    // yet, create it automatically (create-if-not-missing).
    let db = if let Some(ref pw) = args.passphrase {
        Arc::new(Database::open_encrypted(&args.db_dir, pw).unwrap_or_else(|e| {
            eprintln!("failed to open {}: {e}", args.db_dir);
            std::process::exit(1);
        }))
    } else {
        Arc::new(
            Database::open(&args.db_dir)
                .or_else(|_| Database::create(&args.db_dir))
                .unwrap_or_else(|e| {
                    eprintln!("failed to open or create {}: {e}", args.db_dir);
                    std::process::exit(1);
                }),
        )
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

        let mut sigterm =
            signal(SignalKind::terminate()).expect("install SIGTERM handler");
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

/// Flush all tables and remove the pidfile (if we wrote one).
fn shutdown(db: &Arc<Database>, pidfile: &str, daemon: bool) {
    let _ = db.close();
    eprintln!("shutdown complete");
    if daemon {
        let _ = std::fs::remove_file(pidfile);
    }
}
