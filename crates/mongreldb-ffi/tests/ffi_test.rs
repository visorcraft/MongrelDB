//! FFI round-trip tests: exercise the C ABI from Rust.
//!
//! These tests link against the rlib and call the `extern "C"` functions
//! directly, verifying that the FFI layer correctly marshals types.

use mongreldb_ffi::*;
use std::ffi::{CStr, CString};

fn make_tempdir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mdb_ffi_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cstr(s: &str) -> *const std::os::raw::c_char {
    CString::new(s).unwrap().into_raw()
}

unsafe fn rust_str(p: *const std::os::raw::c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    CStr::from_ptr(p).to_string_lossy().into_owned()
}

unsafe fn make_test_db(path: &str) -> mongreldb_database_t {
    let db = mongreldb_create(cstr(path));
    assert!(
        !db.is_null(),
        "create failed: {}",
        rust_str(mongreldb_last_error())
    );
    db
}

unsafe fn make_simple_table(db: mongreldb_database_t, name: &str) {
    let builder = mongreldb_schema_begin();
    assert!(!builder.is_null());
    let col1 = mongreldb_column_def {
        id: 1,
        name: cstr("id"),
        ty: mongreldb_type_id::Int64,
        flags: MONGRELDB_COL_PRIMARY_KEY,
        embedding_dim: 0,
        decimal_precision: 0,
        decimal_scale: 0,
        enum_variants: StringArray {
            items: std::ptr::null(),
            len: 0,
        },
    };
    let col2 = mongreldb_column_def {
        id: 2,
        name: cstr("name"),
        ty: mongreldb_type_id::Bytes,
        flags: MONGRELDB_COL_NULLABLE,
        embedding_dim: 0,
        decimal_precision: 0,
        decimal_scale: 0,
        enum_variants: StringArray {
            items: std::ptr::null(),
            len: 0,
        },
    };
    assert_eq!(mongreldb_schema_add_column(builder, &col1), 0);
    assert_eq!(mongreldb_schema_add_column(builder, &col2), 0);
    let schema = mongreldb_schema_build(builder);
    assert!(!schema.is_null());
    mongreldb_schema_builder_free(builder);
    let mut table_id: u64 = 0;
    let ret = mongreldb_create_table(db, cstr(name), schema, &mut table_id);
    assert_eq!(
        ret,
        0,
        "create_table failed: {}",
        rust_str(mongreldb_last_error())
    );
}

unsafe fn make_cell_input_array(cells: &mut [mongreldb_cell_input]) -> mongreldb_cell_input_array {
    mongreldb_cell_input_array {
        data: cells.as_ptr(),
        len: cells.len(),
    }
}

#[test]
fn ffi_database_create_open_close() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);
        mongreldb_database_free(db);

        let db = mongreldb_open(cstr(path));
        assert!(
            !db.is_null(),
            "open failed: {}",
            rust_str(mongreldb_last_error())
        );
        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_create_table_and_count() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);
        make_simple_table(db, "users");

        let table = mongreldb_database_table(db, cstr("users"));
        assert!(
            !table.is_null(),
            "table lookup failed: {}",
            rust_str(mongreldb_last_error())
        );

        let mut count: u64 = 0;
        let ret = mongreldb_table_count(table, &mut count);
        assert_eq!(ret, 0);
        assert_eq!(count, 0);

        mongreldb_table_free(table);
        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_put_and_query() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);
        make_simple_table(db, "items");

        let table = mongreldb_database_table(db, cstr("items"));
        assert!(!table.is_null());

        // Put a row: id=42, name="hello"
        let hello = b"hello".to_vec();
        let mut cells: [mongreldb_cell_input; 2] = [
            mongreldb_cell_input {
                column_id: 1,
                value: CValue::int64(42),
            },
            mongreldb_cell_input {
                column_id: 2,
                value: CValue {
                    tag: CValueTag::Bytes,
                    payload: CValuePayload {
                        bytes: ByteSlice::from_slice(&hello),
                    },
                },
            },
        ];
        let cell_arr = make_cell_input_array(&mut cells);

        let mut row_id: u64 = 0;
        let ret = mongreldb_table_put(table, &cell_arr, &mut row_id);
        assert_eq!(ret, 0, "put failed: {}", rust_str(mongreldb_last_error()));

        let mut count: u64 = 0;
        mongreldb_table_count(table, &mut count);
        assert_eq!(count, 1);

        // Query all (no conditions).
        let query = mongreldb_query_begin();
        let result = mongreldb_table_query(table, query);
        assert!(
            !result.is_null(),
            "query failed: {}",
            rust_str(mongreldb_last_error())
        );
        assert_eq!(mongreldb_result_count(result), 1);

        mongreldb_result_free(result);
        mongreldb_query_free(query);
        mongreldb_table_free(table);
        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_transaction_put_commit() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);
        make_simple_table(db, "txn_test");

        let txn = mongreldb_begin(db);
        assert!(!txn.is_null());

        for i in 1i64..=3 {
            let mut cells = [mongreldb_cell_input {
                column_id: 1,
                value: CValue::int64(i),
            }];
            let cell_arr = make_cell_input_array(&mut cells);
            let ret = mongreldb_txn_put(txn, cstr("txn_test"), &cell_arr);
            assert_eq!(
                ret,
                0,
                "txn put failed: {}",
                rust_str(mongreldb_last_error())
            );
        }

        let mut epoch: u64 = 0;
        let ret = mongreldb_txn_commit(txn, &mut epoch);
        assert_eq!(
            ret,
            0,
            "commit failed: {}",
            rust_str(mongreldb_last_error())
        );

        let table = mongreldb_database_table(db, cstr("txn_test"));
        let mut count: u64 = 0;
        mongreldb_table_count(table, &mut count);
        assert_eq!(count, 3);

        mongreldb_table_free(table);
        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_error_on_nonexistent_table() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);

        let table = mongreldb_database_table(db, cstr("nonexistent"));
        assert!(table.is_null(), "should fail on nonexistent table");

        let err = rust_str(mongreldb_last_error());
        assert!(!err.is_empty(), "error message should be set");

        let code = mongreldb_last_error_code();
        assert!(code < 0, "error code should be negative, got {}", code);

        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_auth_create_user_and_verify() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);

        let ret = mongreldb_create_user(db, cstr("alice"), cstr("s3cret"));
        assert_eq!(
            ret,
            0,
            "create_user failed: {}",
            rust_str(mongreldb_last_error())
        );

        let mut ok: u8 = 0;
        let ret = mongreldb_verify_user(db, cstr("alice"), cstr("s3cret"), &mut ok);
        assert_eq!(ret, 0);
        assert_eq!(ok, 1, "verify_user should return 1 for correct password");

        let ret = mongreldb_verify_user(db, cstr("alice"), cstr("wrong"), &mut ok);
        assert_eq!(ret, 0);
        assert_eq!(ok, 0, "verify_user should return 0 for wrong password");

        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
