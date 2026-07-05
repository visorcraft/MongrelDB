//! Multi-process file locking: a second `Database::open` on the same directory
//! must fail with a clear error.

use mongreldb_core::Database;
use tempfile::tempdir;

#[test]
fn second_open_of_same_dir_fails() {
    let dir = tempdir().unwrap();
    let _db1 = Database::create(dir.path()).unwrap();
    // A second open on the same directory must fail (the first holds the lock).
    match Database::open(dir.path()) {
        Ok(_) => panic!("second open should fail due to file lock, but succeeded"),
        Err(e) => {
            let msg = e.to_string();
            assert!(msg.contains("locked"), "error should mention 'locked', got: {msg}");
        }
    }
}

#[test]
fn open_after_drop_succeeds() {
    let dir = tempdir().unwrap();
    {
        let _db = Database::create(dir.path()).unwrap();
    } // db dropped → lock released
    // Now a fresh open should succeed.
    let _db2 = Database::open(dir.path()).unwrap();
}
