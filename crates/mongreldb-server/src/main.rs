//! mongreldb-server entry point.

use mongreldb_server::build_app;
use std::net::SocketAddr;
use std::sync::Arc;

use mongreldb_core::Database;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <db_dir> [port]", args[0]);
        std::process::exit(1);
    }
    let db_dir = &args[1];
    let port: u16 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8453);

    let db = Database::open(db_dir).unwrap_or_else(|e| {
        eprintln!("failed to open {db_dir}: {e}");
        std::process::exit(1);
    });
    let app = build_app(Arc::new(db));

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    eprintln!("mongreldb-server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
