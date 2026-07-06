//! mongreldb-server entry point.

use mongreldb_server::spawn_auto_compactor;
use std::net::SocketAddr;
use std::sync::Arc;

use mongreldb_core::Database;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <db_dir> [port] [--auth-token <token>] [--max-connections <n>]", args[0]);
        std::process::exit(1);
    }
    let db_dir = &args[1];
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8453);

    // Parse optional flags from the remaining args.
    let mut auth_token: Option<String> = None;
    let mut max_connections: Option<usize> = None;
    let mut user_auth = false;
    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--auth-token" => {
                auth_token = args.get(i + 1).cloned();
                i += 2;
            }
            "--max-connections" => {
                max_connections = args.get(i + 1).and_then(|s| s.parse().ok());
                i += 2;
            }
            "--auth-users" => {
                user_auth = true;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }

    if auth_token.is_some() {
        eprintln!("token authentication enabled (Authorization: Bearer <token>)");
    }
    if user_auth {
        eprintln!("user authentication enabled (Authorization: Basic <user:pass>)");
    }
    if let Some(max) = max_connections {
        eprintln!("connection limit: {max}");
    }

    let db = Arc::new(Database::open(db_dir).unwrap_or_else(|e| {
        eprintln!("failed to open {db_dir}: {e}");
        std::process::exit(1);
    }));
    // §5.9: background cost-aware compaction (run-count trigger).
    spawn_auto_compactor(Arc::clone(&db));
    let app = mongreldb_server::build_app_full(
        db,
        std::iter::empty(),
        auth_token,
        max_connections,
        user_auth,
    );

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    eprintln!("mongreldb-server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
