//! Kit FFI round-trip tests: exercise the Kit C ABI from Rust.
//!
//! These tests link against the rlib and call the `extern "C"` functions
//! directly, verifying SQL, migrations, and query builder execution.

use mongreldb_kit::*;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;

fn make_tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mdb_kit_ffi_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cstr(s: &str) -> *const c_char {
    CString::new(s).unwrap().into_raw()
}

unsafe fn rust_str(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    CStr::from_ptr(p).to_string_lossy().into_owned()
}

/// A minimal Kit schema with one table: users(id INT64 PK, name TEXT).
const SCHEMA_JSON: &str = r#"{
    "tables": [{
        "id": 1,
        "name": "users",
        "columns": [
            {"id": 1, "name": "id", "storage_type": "int64", "application_type": "int64", "nullable": false, "primary_key": true, "default": null, "generated": false},
            {"id": 2, "name": "name", "storage_type": "text", "application_type": "text", "nullable": true, "primary_key": false, "default": null, "generated": false}
        ],
        "primary_key": ["id"]
    }]
}"#;

unsafe fn make_kit_db(path: &str) -> mongreldb_kit_database_t {
    let db = mongreldb_kit_create(cstr(path), cstr(SCHEMA_JSON));
    assert!(
        !db.is_null(),
        "kit_create failed: {}",
        rust_str(mongreldb_kit_last_error())
    );
    db
}

#[test]
fn kit_ffi_create_and_open() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_kit_db(path);
        mongreldb_kit_database_free(db);

        // Re-open.
        let db = mongreldb_kit_open(cstr(path));
        assert!(
            !db.is_null(),
            "kit_open failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        mongreldb_kit_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kit_ffi_sql_rows() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_kit_db(path);

        // Insert a row via SQL.
        let mut out: *const c_char = std::ptr::null();
        let ret = mongreldb_kit_sql_rows(
            db,
            cstr("INSERT INTO users (id, name) VALUES (1, 'alice')"),
            &mut out,
        );
        assert_eq!(
            ret,
            0,
            "INSERT failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        mongreldb_kit_free_json(out as *mut _);

        // SELECT via sql_rows → JSON array of row objects.
        let ret = mongreldb_kit_sql_rows(db, cstr("SELECT id, name FROM users"), &mut out);
        assert_eq!(
            ret,
            0,
            "SELECT failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        let json = rust_str(out);
        mongreldb_kit_free_json(out as *mut _);

        assert!(
            json.contains("alice"),
            "JSON should contain 'alice': {}",
            json
        );
        assert!(
            json.contains("\"id\""),
            "JSON should contain id column: {}",
            json
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kit_ffi_sql_arrow() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_kit_db(path);

        // Insert via SQL first.
        let mut json_out: *const c_char = std::ptr::null();
        mongreldb_kit_sql_rows(
            db,
            cstr("INSERT INTO users (id, name) VALUES (1, 'bob')"),
            &mut json_out,
        );
        mongreldb_kit_free_json(json_out as *mut _);

        // SELECT via sql_arrow → Arrow IPC file bytes.
        let mut buf: *mut u8 = std::ptr::null_mut();
        let mut len: usize = 0;
        let ret = mongreldb_kit_sql_arrow(db, cstr("SELECT id FROM users"), &mut buf, &mut len);
        assert_eq!(
            ret,
            0,
            "sql_arrow failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        assert!(len >= 6, "Arrow IPC should be at least 6 bytes");

        // Verify ARROW1 magic.
        let magic = std::slice::from_raw_parts(buf, 6);
        assert_eq!(&magic[..6], b"ARROW1", "should start with ARROW1 magic");
        mongreldb_kit_free_arrow(buf, len);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kit_ffi_query_select() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_kit_db(path);

        // Insert via SQL.
        let mut out: *const c_char = std::ptr::null();
        mongreldb_kit_sql_rows(
            db,
            cstr("INSERT INTO users (id, name) VALUES (1, 'carol')"),
            &mut out,
        );
        mongreldb_kit_free_json(out as *mut _);

        // Query via Kit query builder: SELECT * FROM users
        let select_json = r#"{"table":"users","columns":[],"filter":null,"order_by":[],"limit":null,"offset":null}"#;
        let ret = mongreldb_kit_query_select_json(db, cstr(select_json), &mut out);
        assert_eq!(
            ret,
            0,
            "select failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        let json = rust_str(out);
        mongreldb_kit_free_json(out as *mut _);

        assert!(
            json.contains("carol"),
            "result should contain 'carol': {}",
            json
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kit_ffi_migrate() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_kit_db(path);

        // Run a migration that creates a new table via raw_sql.
        let migrations_json = r#"[{
            "version": 1,
            "name": "add_orders",
            "ops": [{"raw_sql": "CREATE TABLE orders (id INT64 PRIMARY KEY, total FLOAT64)"}]
        }]"#;
        let ret = mongreldb_kit_migrate_json(db, cstr(migrations_json));
        assert_eq!(
            ret,
            0,
            "migrate failed: {}",
            rust_str(mongreldb_kit_last_error())
        );

        // Verify the table was created by inserting into it.
        let mut out: *const c_char = std::ptr::null();
        let ret = mongreldb_kit_sql_rows(
            db,
            cstr("INSERT INTO orders (id, total) VALUES (1, 99.99)"),
            &mut out,
        );
        assert_eq!(
            ret,
            0,
            "INSERT into migrated table failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        mongreldb_kit_free_json(out as *mut _);

        // Read back applied migrations.
        let ret = mongreldb_kit_applied_migrations_json(db, &mut out);
        assert_eq!(
            ret,
            0,
            "applied_migrations failed: {}",
            rust_str(mongreldb_kit_last_error())
        );
        let json = rust_str(out);
        mongreldb_kit_free_json(out as *mut _);
        assert!(
            json.contains("add_orders"),
            "applied migrations should contain 'add_orders': {}",
            json
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kit_ffi_error_handling() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_kit_db(path);

        // Invalid SQL should return a negative error code with a message.
        let mut out: *const c_char = std::ptr::null();
        let ret = mongreldb_kit_sql_rows(db, cstr("SELECT * FROM nonexistent"), &mut out);
        assert!(ret < 0, "invalid SQL should return error, got {}", ret);
        assert!(
            !rust_str(mongreldb_kit_last_error()).is_empty(),
            "error message should be set"
        );
        mongreldb_kit_free_json(out as *mut _);

        mongreldb_kit_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
