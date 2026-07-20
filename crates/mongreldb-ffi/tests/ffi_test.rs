//! FFI round-trip tests: exercise the C ABI from Rust.
//!
//! These tests link against the rlib and call the `extern "C"` functions
//! directly, verifying that the FFI layer correctly marshals types.

use mongreldb::*;
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

#[test]
fn ffi_minhash_golden_fixture_matches_v1() {
    let golden: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("../../../docs/ai/minhash-v1-golden.json")).unwrap();
    for fixture in golden {
        let member = serde_json::to_string(&fixture["member"]).unwrap();
        let mut hash = 0;
        let result = unsafe { mongreldb_minhash_member_hash_v1_json(cstr(&member), &mut hash) };
        assert_eq!(result, 0);
        assert_eq!(hash.to_string(), fixture["expected"]);
    }
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

unsafe fn make_simple_table(db: mongreldb_database_t, name: &str) -> u64 {
    let builder = mongreldb_schema_begin();
    assert!(!builder.is_null());
    let col1 = mongreldb_column_def {
        id: 1,
        name: cstr("id"),
        ty: mongreldb_type_id::Int64 as i32,
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
        ty: mongreldb_type_id::Bytes as i32,
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
    table_id
}

unsafe fn make_cell_input_array(cells: &mut [mongreldb_cell_input]) -> mongreldb_cell_input_array {
    mongreldb_cell_input_array {
        data: cells.as_ptr(),
        len: cells.len(),
    }
}

#[test]
fn ffi_specialized_column_rejects_wrong_value_before_write() {
    let dir = make_tempdir();
    unsafe {
        let db = make_test_db(dir.to_str().unwrap());
        let builder = mongreldb_schema_begin();
        for column in [
            mongreldb_column_def {
                id: 1,
                name: cstr("id"),
                ty: mongreldb_type_id::Int64 as i32,
                flags: MONGRELDB_COL_PRIMARY_KEY,
                embedding_dim: 0,
                decimal_precision: 0,
                decimal_scale: 0,
                enum_variants: StringArray::default(),
            },
            mongreldb_column_def {
                id: 2,
                name: cstr("sparse"),
                ty: mongreldb_type_id::Bytes as i32,
                flags: 0,
                embedding_dim: 0,
                decimal_precision: 0,
                decimal_scale: 0,
                enum_variants: StringArray::default(),
            },
        ] {
            assert_eq!(mongreldb_schema_add_column(builder, &column), 0);
        }
        assert_eq!(
            mongreldb_schema_add_index(
                builder,
                &mongreldb_index_def {
                    name: cstr("sparse_idx"),
                    column_id: 2,
                    kind: mongreldb_index_kind::Sparse as i32,
                },
            ),
            0
        );
        let schema = mongreldb_schema_build(builder);
        let mut table_id = 0;
        assert_eq!(
            mongreldb_create_table(db, cstr("docs"), schema, &mut table_id),
            0
        );
        let table = mongreldb_database_table(db, cstr("docs"));
        let mut cells = [
            mongreldb_cell_input {
                column_id: 1,
                value: CValue::int64(1),
            },
            mongreldb_cell_input {
                column_id: 2,
                value: CValue::int64(7),
            },
        ];
        let cells = make_cell_input_array(&mut cells);
        assert_eq!(mongreldb_table_put(table, &cells, std::ptr::null_mut()), -1);
        let error = rust_str(mongreldb_last_error());
        assert!(error.contains("does not match type Bytes"), "{error}");
        let mut count = 99;
        assert_eq!(mongreldb_table_count(table, &mut count), 0);
        assert_eq!(count, 0);
        mongreldb_table_free(table);
        mongreldb_database_free(db);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn ffi_exact_ann_rerank_returns_scored_hit() {
    let dir = make_tempdir();
    unsafe {
        let db = make_test_db(dir.to_str().unwrap());
        let builder = mongreldb_schema_begin();
        for column in [
            mongreldb_column_def {
                id: 1,
                name: cstr("id"),
                ty: mongreldb_type_id::Int64 as i32,
                flags: MONGRELDB_COL_PRIMARY_KEY,
                embedding_dim: 0,
                decimal_precision: 0,
                decimal_scale: 0,
                enum_variants: StringArray::default(),
            },
            mongreldb_column_def {
                id: 2,
                name: cstr("embedding"),
                ty: mongreldb_type_id::Embedding as i32,
                flags: MONGRELDB_COL_EMBEDDING_BINARY_QUANTIZED,
                embedding_dim: 2,
                decimal_precision: 0,
                decimal_scale: 0,
                enum_variants: StringArray::default(),
            },
        ] {
            assert_eq!(mongreldb_schema_add_column(builder, &column), 0);
        }
        assert_eq!(
            mongreldb_schema_add_index(
                builder,
                &mongreldb_index_def {
                    name: cstr("ann_idx"),
                    column_id: 2,
                    kind: mongreldb_index_kind::Ann as i32,
                },
            ),
            0
        );
        let schema = mongreldb_schema_build(builder);
        let mut table_id = 0;
        assert_eq!(
            mongreldb_create_table(db, cstr("docs"), schema, &mut table_id),
            0
        );
        let table = mongreldb_database_table(db, cstr("docs"));
        let mut first_row_id = 0;
        let mut row_ids = Vec::new();
        for (id, embedding) in [(1, [1.0_f32, 0.0]), (2, [0.0_f32, 1.0])] {
            let mut cells = [
                mongreldb_cell_input {
                    column_id: 1,
                    value: CValue::int64(id),
                },
                mongreldb_cell_input {
                    column_id: 2,
                    value: CValue {
                        tag: CValueTag::Embedding as i32,
                        payload: CValuePayload {
                            embedding: EmbeddingSlice {
                                data: embedding.as_ptr(),
                                len: embedding.len(),
                            },
                        },
                    },
                },
            ];
            let cells = make_cell_input_array(&mut cells);
            let mut row_id = 0;
            assert_eq!(mongreldb_table_put(table, &cells, &mut row_id), 0);
            row_ids.push(row_id);
            if id == 1 {
                first_row_id = row_id;
            }
        }
        assert_ne!(row_ids[0], row_ids[1]);

        let query = [1.0_f32, 0.0];
        let result = mongreldb_table_ann_rerank(
            table,
            2,
            EmbeddingSlice {
                data: query.as_ptr(),
                len: query.len(),
            },
            2,
            1,
            mongreldb_vector_metric::Cosine as i32,
        );
        assert!(
            !result.is_null(),
            "rerank failed: {}",
            rust_str(mongreldb_last_error())
        );
        assert_eq!(mongreldb_ann_rerank_result_count(result), 1);
        let mut hit = mongreldb_ann_rerank_hit::default();
        assert_eq!(mongreldb_ann_rerank_result_hit(result, 0, &mut hit), 0);
        assert_eq!(hit.row_id, first_row_id);
        assert_eq!(
            hit.candidate_distance_kind,
            mongreldb_ann_candidate_distance_kind::Hamming as i32
        );
        assert_eq!(hit.exact_score, 1.0);
        mongreldb_ann_rerank_result_free(result);

        let invalid = mongreldb_table_ann_rerank(
            table,
            2,
            EmbeddingSlice {
                data: query.as_ptr(),
                len: query.len(),
            },
            2,
            1,
            99,
        );
        assert!(invalid.is_null());

        mongreldb_table_free(table);
        mongreldb_database_free(db);
    }
    let _ = std::fs::remove_dir_all(dir);
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
                    tag: CValueTag::Bytes as i32,
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
        let committed_epoch = epoch;

        epoch = 0;
        assert_eq!(mongreldb_txn_commit(txn, &mut epoch), 0);
        assert_eq!(epoch, committed_epoch);
        let mut late_cells = [mongreldb_cell_input {
            column_id: 1,
            value: CValue::int64(4),
        }];
        let late_cell_arr = make_cell_input_array(&mut late_cells);
        assert_eq!(mongreldb_txn_put(txn, cstr("txn_test"), &late_cell_arr), -1);
        let mut details: mongreldb_error_details_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_last_error_details_v1(&mut details), 0);
        assert_eq!(details.outcome_known, 1);
        assert_eq!(details.committed, 1);
        assert_eq!(details.last_commit_epoch, committed_epoch);
        assert_eq!(mongreldb_txn_rollback(txn), -1);

        let table = mongreldb_database_table(db, cstr("txn_test"));
        let mut count: u64 = 0;
        mongreldb_table_count(table, &mut count);
        assert_eq!(count, 3);

        mongreldb_table_free(table);
        mongreldb_txn_free(txn);
        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_transaction_rollback_is_terminal() {
    let dir = make_tempdir();

    unsafe {
        let db = make_test_db(dir.to_str().unwrap());
        make_simple_table(db, "txn_rollback");

        let txn = mongreldb_begin(db);
        let mut cells = [mongreldb_cell_input {
            column_id: 1,
            value: CValue::int64(1),
        }];
        let cell_arr = make_cell_input_array(&mut cells);
        assert_eq!(mongreldb_txn_put(txn, cstr("txn_rollback"), &cell_arr), 0);
        assert_eq!(mongreldb_txn_rollback(txn), 0);
        assert_eq!(mongreldb_txn_rollback(txn), 0);

        assert_eq!(mongreldb_txn_put(txn, cstr("txn_rollback"), &cell_arr), -1);
        let mut details: mongreldb_error_details_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_last_error_details_v1(&mut details), 0);
        assert_eq!(details.outcome_known, 1);
        assert_eq!(details.committed, 0);

        let mut epoch = 0;
        assert_eq!(mongreldb_txn_commit(txn, &mut epoch), -1);
        assert_eq!(epoch, 0);

        let table = mongreldb_database_table(db, cstr("txn_rollback"));
        let mut count = 99;
        assert_eq!(mongreldb_table_count(table, &mut count), 0);
        assert_eq!(count, 0);

        mongreldb_table_free(table);
        mongreldb_txn_free(txn);
        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_transaction_durable_error_is_terminal_and_structured() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);
        let table_id = make_simple_table(db, "txn_durable");
        let manifest = dir.join("tables").join(table_id.to_string()).join("_mf");
        let original_manifest = std::fs::read(&manifest).unwrap();
        std::fs::remove_file(&manifest).unwrap();
        std::fs::create_dir(&manifest).unwrap();

        let txn = mongreldb_begin(db);
        let mut cells = [mongreldb_cell_input {
            column_id: 1,
            value: CValue::int64(1),
        }];
        let cell_arr = make_cell_input_array(&mut cells);
        assert_eq!(mongreldb_txn_put(txn, cstr("txn_durable"), &cell_arr), 0);

        let mut epoch = 0;
        assert_eq!(
            mongreldb_txn_commit(txn, &mut epoch),
            -15,
            "expected durable outcome: {}",
            rust_str(mongreldb_last_error())
        );
        assert!(epoch > 0);

        let mut details: mongreldb_error_details_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_last_error_details_v1(&mut details), 0);
        assert_eq!(details.struct_size, std::mem::size_of_val(&details));
        assert_eq!(details.version, 1);
        assert_eq!(details.code, -15);
        assert_eq!(details.outcome_known, 1);
        assert_eq!(details.committed, 1);
        assert_eq!(details.has_last_commit_epoch, 1);
        assert_eq!(details.last_commit_epoch, epoch);
        assert_eq!(details.retryable, 0);

        epoch = 0;
        for operation in 0..3 {
            let repeated = match operation {
                0 => mongreldb_txn_commit(txn, &mut epoch),
                1 => mongreldb_txn_put(txn, cstr("txn_durable"), &cell_arr),
                _ => mongreldb_txn_rollback(txn),
            };
            assert_eq!(repeated, -15);
            let mut repeated_details: mongreldb_error_details_v1 = std::mem::zeroed();
            assert_eq!(mongreldb_last_error_details_v1(&mut repeated_details), 0);
            assert_eq!(repeated_details.outcome_known, 1);
            assert_eq!(repeated_details.committed, 1);
            assert_eq!(repeated_details.has_last_commit_epoch, 1);
            assert_eq!(repeated_details.last_commit_epoch, epoch);
            assert_eq!(repeated_details.retryable, 0);
        }

        mongreldb_txn_free(txn);
        mongreldb_database_free(db);
        std::fs::remove_dir(&manifest).unwrap();
        std::fs::write(&manifest, original_manifest).unwrap();

        let reopened = mongreldb_open(cstr(path));
        assert!(!reopened.is_null());
        let table = mongreldb_database_table(reopened, cstr("txn_durable"));
        let mut count = 0;
        assert_eq!(mongreldb_table_count(table, &mut count), 0);
        assert_eq!(count, 1, "terminal commit must not replay staged writes");
        mongreldb_table_free(table);
        mongreldb_database_free(reopened);
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

        // FND-007: NotFound maps to taxonomy StaleMetadata (code 3).
        assert_eq!(mongreldb_last_error_category_code(), 3);
        assert_eq!(rust_str(mongreldb_last_error_category()), "stale metadata");

        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_taxonomy_permission_denied_category_code() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        // Bootstrap require_auth with an admin, then open as a user lacking grants.
        let db = mongreldb_create(cstr(path));
        assert!(!db.is_null());
        assert_eq!(
            mongreldb_enable_auth(db, cstr("admin"), cstr("admin-pw")),
            0,
            "{}",
            rust_str(mongreldb_last_error())
        );
        assert_eq!(mongreldb_create_user(db, cstr("bob"), cstr("bob-pw")), 0);
        mongreldb_database_free(db);

        let bob = mongreldb_open_with_credentials(cstr(path), cstr("bob"), cstr("bob-pw"));
        assert!(!bob.is_null(), "{}", rust_str(mongreldb_last_error()));

        // Creating a table requires Admin/DDL — bob has no roles.
        let builder = mongreldb_schema_begin();
        let col = mongreldb_column_def {
            id: 1,
            name: cstr("id"),
            ty: mongreldb_type_id::Int64 as i32,
            flags: MONGRELDB_COL_PRIMARY_KEY,
            embedding_dim: 0,
            decimal_precision: 0,
            decimal_scale: 0,
            enum_variants: StringArray {
                items: std::ptr::null(),
                len: 0,
            },
        };
        assert_eq!(mongreldb_schema_add_column(builder, &col), 0);
        let schema = mongreldb_schema_build(builder);
        mongreldb_schema_builder_free(builder);
        let mut table_id = 0u64;
        let ret = mongreldb_create_table(bob, cstr("t"), schema, &mut table_id);
        assert_eq!(ret, -6, "expected Unauthorized: {}", rust_str(mongreldb_last_error()));
        // Taxonomy distinguishes permission denied (20) from unauthenticated (19).
        assert_eq!(mongreldb_last_error_category_code(), 20);
        assert_eq!(
            rust_str(mongreldb_last_error_category()),
            "permission denied"
        );
        let mut details = mongreldb_error_details_v1::default();
        assert_eq!(mongreldb_last_error_details_v1(&mut details), 0);
        assert_eq!(details.category_code, 20);

        mongreldb_database_free(bob);
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

// ── SQL execution tests ────────────────────────────────────────────────────

/// Helper: run SQL and return the IPC bytes (freeing is caller's job).
unsafe fn run_sql(db: mongreldb_database_t, sql: &str) -> (i32, *mut u8, usize) {
    let mut buf: *mut u8 = std::ptr::null_mut();
    let mut len: usize = 0;
    let ret = mongreldb_database_sql(db, cstr(sql), &mut buf, &mut len);
    (ret, buf, len)
}

#[test]
fn ffi_sql_ddl_returns_empty_or_nonempty() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);

        // CREATE TABLE via SQL.
        let (ret, buf, len) = run_sql(db, "CREATE TABLE items (id INT64 PRIMARY KEY, qty INT64)");
        assert_eq!(
            ret,
            0,
            "CREATE TABLE via SQL failed: {}",
            rust_str(mongreldb_last_error())
        );
        // DDL produces no result rows: zero-length IPC buffer.
        assert_eq!(len, 0, "DDL should produce empty IPC buffer");
        mongreldb_free_sql_result(buf, len);

        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_sql_insert_and_select() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);

        let (ret, _, _) = run_sql(
            db,
            "CREATE TABLE products (id INT64 PRIMARY KEY, name VARCHAR, price FLOAT64)",
        );
        assert_eq!(
            ret,
            0,
            "CREATE failed: {}",
            rust_str(mongreldb_last_error())
        );

        // Insert two rows via SQL.
        let (ret, _, _) = run_sql(
            db,
            "INSERT INTO products (id, name, price) VALUES (1, 'widget', 9.99)",
        );
        assert_eq!(
            ret,
            0,
            "INSERT 1 failed: {}",
            rust_str(mongreldb_last_error())
        );

        let (ret, _, _) = run_sql(
            db,
            "INSERT INTO products (id, name, price) VALUES (2, 'gadget', 19.99)",
        );
        assert_eq!(
            ret,
            0,
            "INSERT 2 failed: {}",
            rust_str(mongreldb_last_error())
        );

        // SELECT should return Arrow IPC file bytes (non-empty, starts with
        // the ARROW1 magic).
        let (ret, buf, len) = run_sql(db, "SELECT id, name, price FROM products ORDER BY id");
        assert_eq!(
            ret,
            0,
            "SELECT failed: {}",
            rust_str(mongreldb_last_error())
        );
        assert!(len > 0, "SELECT should produce non-empty IPC buffer");
        assert!(len >= 6, "IPC buffer too small to contain ARROW1 magic");
        // Arrow IPC file format starts with "ARROW1\0".
        let magic = std::slice::from_raw_parts(buf, 6);
        assert_eq!(
            &magic[..6],
            b"ARROW1",
            "IPC buffer should start with ARROW1 magic"
        );
        mongreldb_free_sql_result(buf, len);

        // Verify the row count via SQL.
        let (ret, buf, len) = run_sql(db, "SELECT COUNT() AS n FROM products");
        assert_eq!(ret, 0, "COUNT failed: {}", rust_str(mongreldb_last_error()));
        assert!(len > 0, "COUNT should produce IPC output");
        mongreldb_free_sql_result(buf, len);

        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ffi_sql_error_handling() {
    let dir = make_tempdir();
    let path = dir.to_str().unwrap();

    unsafe {
        let db = make_test_db(path);

        // Invalid SQL should return a negative error code with a message.
        let (ret, buf, len) = run_sql(db, "SELECT FROM nonexistent_table");
        assert!(
            ret < 0,
            "invalid SQL should return negative error code, got {}",
            ret
        );
        let msg = rust_str(mongreldb_last_error());
        assert!(
            !msg.is_empty(),
            "error message should be set for failed SQL"
        );
        // On error, the buffer should not have been allocated.
        mongreldb_free_sql_result(buf, len);

        mongreldb_database_free(db);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Migration planning tests ───────────────────────────────────────────────

#[test]
fn ffi_migration_checksum() {
    unsafe {
        // Compute the checksum for a single create_table migration.
        let ops = r#"[{"create_table":{"name":"users"}}]"#;
        let mut out: *const std::os::raw::c_char = std::ptr::null();
        let ret = mongreldb_migration_checksum_json(1, cstr("initial"), cstr(ops), &mut out);
        assert_eq!(
            ret,
            0,
            "checksum failed: {}",
            rust_str(mongreldb_last_error())
        );
        let checksum = rust_str(out);
        mongreldb_free_migrate_string(out as *mut _);

        // SHA-256 hex = 64 chars.
        assert_eq!(checksum.len(), 64, "checksum should be 64 hex chars");
        assert!(
            checksum.chars().all(|c| c.is_ascii_hexdigit()),
            "checksum should be hex: {}",
            checksum
        );
    }
}

#[test]
fn ffi_migration_plan() {
    unsafe {
        // No applied migrations → all desired are pending.
        let applied = "[]";
        let desired = r#"[
            {"version":1,"name":"initial","ops":[{"create_table":{"name":"users"}}]},
            {"version":2,"name":"add_index","ops":[{"add_index":{"table":"users","index":"idx_email"}}]}
        ]"#;

        let mut out: *const std::os::raw::c_char = std::ptr::null();
        let ret = mongreldb_plan_migrations_json(cstr(applied), cstr(desired), &mut out);
        assert_eq!(ret, 0, "plan failed: {}", rust_str(mongreldb_last_error()));
        let result = rust_str(out);
        mongreldb_free_migrate_string(out as *mut _);

        // Both migrations should be pending (no applied).
        assert!(
            result.contains("\"version\":1"),
            "result should contain version 1: {}",
            result
        );
        assert!(
            result.contains("\"version\":2"),
            "result should contain version 2: {}",
            result
        );

        // Now with version 1 applied, only version 2 should be pending.
        let applied1 = r#"[{"version":1,"name":"initial","ops":[]}]"#;
        let ret = mongreldb_plan_migrations_json(cstr(applied1), cstr(desired), &mut out);
        assert_eq!(ret, 0);
        let result2 = rust_str(out);
        mongreldb_free_migrate_string(out as *mut _);

        assert!(
            !result2.contains("\"version\":1"),
            "version 1 should not be pending: {}",
            result2
        );
        assert!(
            result2.contains("\"version\":2"),
            "version 2 should be pending: {}",
            result2
        );
    }
}

#[test]
fn ffi_migration_invalid_json() {
    unsafe {
        let mut out: *const std::os::raw::c_char = std::ptr::null();
        let ret = mongreldb_plan_migrations_json(cstr("not json"), cstr("[]"), &mut out);
        assert!(ret < 0, "invalid JSON should return error, got {}", ret);
        assert!(
            !rust_str(mongreldb_last_error()).is_empty(),
            "error message should be set"
        );
    }
}

#[test]
fn ffi_sql_query_handle_wait_cancel_and_reuse() {
    unsafe {
        let path = make_tempdir();
        let db = mongreldb_create(cstr(path.to_str().unwrap()));
        assert!(!db.is_null());

        let query_id = CString::new("00112233445566778899aabbccddeeff").unwrap();
        let options = mongreldb_sql_options {
            query_id: query_id.as_ptr(),
            timeout_ms: 5_000,
        };
        let query = mongreldb_sql_query_start(db, cstr("SELECT 1"), &options);
        assert!(!query.is_null());
        let duplicate = mongreldb_sql_query_start(db, cstr("SELECT 2"), &options);
        assert!(duplicate.is_null());
        assert_eq!(mongreldb_last_error_code(), -11);
        assert!(rust_str(mongreldb_last_error()).contains("already active or retained"));
        let mut result = mongreldb_sql_result_t {
            data: std::ptr::null_mut(),
            len: 0,
        };
        assert_eq!(mongreldb_sql_query_wait(query, &mut result), 0);
        assert!(result.len > 0);
        let mut status: mongreldb_sql_query_status_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_sql_query_get_status(query, &mut status), 0);
        assert_eq!(status.phase, 8);
        assert_eq!(status.terminal_state, 1);
        assert_eq!(status.committed, 0);
        let mut status_v2: mongreldb_sql_query_status_v2 = std::mem::zeroed();
        assert_eq!(mongreldb_sql_query_get_status_v2(query, &mut status_v2), 0);
        assert_eq!(status_v2.outcome_known, 1);
        assert_eq!(status_v2.v1.terminal_state, status.terminal_state);
        assert_eq!(
            rust_str(status.query_id.as_ptr()),
            query_id.to_str().unwrap()
        );
        assert_eq!(mongreldb_sql_query_cancel(query), 4);
        mongreldb_free_sql_result(result.data, result.len);
        mongreldb_sql_query_free(query);

        let limit_id = CString::new("102132435465768798a9bacbdcedfe0f").unwrap();
        let limit_options = mongreldb_sql_options_v2 {
            query_id: limit_id.as_ptr(),
            timeout_ms: 5_000,
            max_output_rows: 1,
            max_output_bytes: 1_024 * 1_024,
        };
        let query = mongreldb_sql_query_start_v2(
            db,
            cstr("SELECT 1 AS id UNION ALL SELECT 2 AS id"),
            &limit_options,
        );
        assert!(!query.is_null());
        let mut limited = mongreldb_sql_result_t {
            data: std::ptr::null_mut(),
            len: 0,
        };
        assert!(mongreldb_sql_query_wait(query, &mut limited) < 0);
        let mut error_details: mongreldb_error_details_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_last_error_details_v1(&mut error_details), 0);
        assert_eq!(error_details.code, -17);
        assert_eq!(error_details.outcome_known, 1);
        assert_eq!(error_details.committed, 0);
        assert_eq!(
            rust_str(error_details.query_id.as_ptr()),
            limit_id.to_str().unwrap()
        );
        let mut status: mongreldb_sql_query_status_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_sql_query_get_status(query, &mut status), 0);
        assert_eq!(status.terminal_error_category, 3);
        assert_eq!(
            rust_str(status.terminal_error_code.as_ptr()),
            "RESULT_LIMIT_EXCEEDED"
        );
        mongreldb_sql_query_free(query);

        let cancel_id = CString::new("ffeeddccbbaa99887766554433221100").unwrap();
        let cancel_options = mongreldb_sql_options {
            query_id: cancel_id.as_ptr(),
            timeout_ms: 30_000,
        };
        let query = mongreldb_sql_query_start(
            db,
            cstr("SELECT count(*) FROM generate_series(1, 1000000000)"),
            &cancel_options,
        );
        assert!(!query.is_null());
        assert_eq!(mongreldb_sql_query_cancel(query), 1);
        let mut cancelled = mongreldb_sql_result_t {
            data: std::ptr::null_mut(),
            len: 0,
        };
        assert!(mongreldb_sql_query_wait(query, &mut cancelled) < 0);
        let mut error_details: mongreldb_error_details_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_last_error_details_v1(&mut error_details), 0);
        assert_eq!(error_details.code, -9);
        assert_eq!(error_details.outcome_known, 1);
        assert_eq!(error_details.committed, 0);
        assert_eq!(error_details.cancellation_reason, 1);
        assert_eq!(
            rust_str(error_details.query_id.as_ptr()),
            cancel_id.to_str().unwrap()
        );
        mongreldb_sql_query_free(query);

        let mut data = std::ptr::null_mut();
        let mut len = 0;
        assert_eq!(
            mongreldb_database_sql(db, cstr("SELECT 2"), &mut data, &mut len),
            0
        );
        mongreldb_free_sql_result(data, len);
        mongreldb_database_free(db);
        std::fs::remove_dir_all(path).ok();
    }
}

#[test]
fn ffi_sql_query_cancel_during_arrow_ipc_encoding() {
    unsafe {
        let path = make_tempdir();
        let db = mongreldb_create(cstr(path.to_str().unwrap()));
        assert!(!db.is_null());
        let query_id = CString::new("11223344556677889900aabbccddeeff").unwrap();
        let options = mongreldb_sql_options_v2 {
            query_id: query_id.as_ptr(),
            timeout_ms: 30_000,
            max_output_rows: 3_000_000,
            max_output_bytes: 256 * 1024 * 1024,
        };
        let query = mongreldb_sql_query_start_v2(
            db,
            cstr("SELECT * FROM generate_series(1, 2000000)"),
            &options,
        );
        assert!(!query.is_null());

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let mut status: mongreldb_sql_query_status_v1 = std::mem::zeroed();
            assert_eq!(mongreldb_sql_query_get_status(query, &mut status), 0);
            if status.phase == 5 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "query never entered SQL serialization; phase={}",
                status.phase
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        assert!(matches!(mongreldb_sql_query_cancel(query), 1 | 2));
        let mut result = mongreldb_sql_result_t {
            data: std::ptr::null_mut(),
            len: 0,
        };
        assert!(mongreldb_sql_query_wait(query, &mut result) < 0);
        assert!(result.data.is_null());
        assert_eq!(result.len, 0);
        let mut status: mongreldb_sql_query_status_v1 = std::mem::zeroed();
        assert_eq!(mongreldb_sql_query_get_status(query, &mut status), 0);
        assert_eq!(status.phase, 10);
        assert_eq!(status.terminal_state, 3);
        assert_eq!(status.cancellation_reason, 1);

        mongreldb_sql_query_free(query);
        mongreldb_database_free(db);
        let _ = std::fs::remove_dir_all(path);
    }
}
