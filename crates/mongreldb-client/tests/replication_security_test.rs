use std::io::{Read, Write};
use std::net::TcpListener;

use mongreldb_client::ReplicationFollower;

fn temporary_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mongreldb-client-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn replication_follower_rejects_unsafe_urls_and_redacts_secrets() {
    let directory = std::env::temp_dir().join("mongreldb-client-replication-security");
    for url in [
        "file:///tmp/leader",
        "http://user:password@leader:8453",
        "http://leader:8453?token=secret",
        "http://leader:8453/#secret",
    ] {
        assert!(ReplicationFollower::new(url, &directory).is_err());
    }

    let follower = ReplicationFollower::new("https://leader:8453/", &directory)
        .unwrap()
        .with_bearer_token("bearer-secret")
        .with_basic_auth("remote-user", "remote-secret")
        .with_local_encryption_passphrase("local-passphrase")
        .with_local_credentials("local-user", "local-secret");
    let debug = format!("{follower:?}");
    assert!(debug.contains("https://leader:8453"));
    assert!(debug.contains("remote-user"));
    assert!(debug.contains("local-user"));
    for secret in [
        "bearer-secret",
        "remote-secret",
        "local-passphrase",
        "local-secret",
    ] {
        assert!(!debug.contains(secret));
    }
}

#[test]
fn replication_snapshot_epoch_header_must_match_payload() {
    let source_path = temporary_path("snapshot-source");
    let target_path = temporary_path("snapshot-target");
    let source = mongreldb_core::Database::create(&source_path).unwrap();
    let snapshot = source.replication_snapshot().unwrap();
    let body = snapshot.encode().unwrap();
    let source_id = snapshot
        .source_id()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let wrong_epoch = snapshot.epoch().saturating_add(1);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = [0_u8; 4096];
        let read = stream.read(&mut request).unwrap();
        assert!(String::from_utf8_lossy(&request[..read]).starts_with("GET /replication/snapshot "));
        write!(
            stream,
            concat!(
                "HTTP/1.1 200 OK\r\n",
                "x-mongreldb-source-id: {}\r\n",
                "x-mongreldb-current-epoch: {}\r\n",
                "Content-Length: {}\r\n",
                "Connection: close\r\n\r\n"
            ),
            source_id,
            wrong_epoch,
            body.len()
        )
        .unwrap();
        stream.write_all(&body).unwrap();
    });

    let error = ReplicationFollower::new(&format!("http://{address}"), &target_path)
        .unwrap()
        .bootstrap()
        .unwrap_err();
    assert!(error.contains("epoch header does not match"));
    assert!(!target_path.exists());
    server.join().unwrap();
    drop(source);
    std::fs::remove_dir_all(source_path).unwrap();
}
