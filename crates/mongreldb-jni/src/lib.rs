//! JNI shim for MongrelDB - in-process embedded engine for the JVM.
//!
//! This crate produces `libmongreldb_jni.{so,dylib,dll}`, loaded by Java/Kotlin/
//! Scala via `System.load()`. It wraps the Kit `Database` directly (no C ABI
//! indirection), mirroring how the NAPI addon wraps `mongreldb-core`.
//!
//! # JNI method mapping
//!
//! Each exported function follows the JNI naming convention:
//! `Java_com_visorcraft_mongreldb_native_1mode_NativeDB_native<method>`.
//!
//! The JVM class `com.visorcraft.mongreldb.native.NativeDB` declares these
//! as `native` methods. The handle (Kit `Database` wrapped in `Rc<RefCell>`)
//! is passed as a `jlong` (reinterpret cast).
//!
//! # Thread safety
//!
//! The handle uses `Rc<RefCell>` (single-threaded). Each thread should create
//! its own `NativeDB` instance. Cross-thread sharing requires a `Mutex`.

use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::{jbyteArray, jlong, jstring};
use jni::JNIEnv;
use mongreldb_kit::Database as KitDatabase;
use mongreldb_kit_core::Migration;
use mongreldb_types::errors::ErrorCategory;
use std::cell::RefCell;
use std::path::Path;
use std::rc::Rc;

/// The Rust-side wrapper behind the jlong handle. Uses Rc<RefCell> so the
/// Kit Database can be borrowed mutably (for migrations and transactions).
struct JniDatabase {
    db: Rc<RefCell<KitDatabase>>,
}

// ── Error taxonomy (FND-007) ──────────────────────────────────────────────

/// Map a Kit error onto the stable Stage 0 taxonomy (spec 9.7). Mirrors the
/// core `MongrelError::category()` precedents used by the C FFI and NAPI.
pub(crate) fn kit_error_category(e: &mongreldb_kit::KitError) -> ErrorCategory {
    use mongreldb_kit::KitError;
    match e {
        KitError::PermissionDenied(_) => ErrorCategory::PermissionDenied,
        KitError::InvalidCredentials(_) | KitError::AuthRequired(_) => {
            ErrorCategory::Unauthenticated
        }
        KitError::AuthNotRequired(_) => ErrorCategory::ClusterVersionMismatch,
        KitError::Conflict(_) | KitError::Duplicate(_) => ErrorCategory::TransactionConflict,
        KitError::ForeignKey(_) | KitError::Restrict(_) | KitError::TransactionAborted { .. } => {
            ErrorCategory::TransactionAborted
        }
        KitError::Cancelled { .. } => ErrorCategory::Cancelled,
        KitError::DeadlineExceeded { .. } => ErrorCategory::DeadlineExceeded,
        KitError::OutcomeUnknown { .. } => ErrorCategory::CommitOutcomeUnknown,
        KitError::CommitOutcome { outcome, .. } => {
            if outcome.committed {
                ErrorCategory::CommitOutcomeUnknown
            } else {
                ErrorCategory::TransactionAborted
            }
        }
        KitError::DatabaseLocked(_)
        | KitError::QueryRegistryFull { .. }
        | KitError::ResultLimitExceeded { .. } => ErrorCategory::ResourceExhausted,
        KitError::Validation(_) | KitError::TriggerValidation(_) | KitError::Migration(_) => {
            ErrorCategory::SchemaVersionMismatch
        }
        KitError::Integrity(_) => ErrorCategory::StaleMetadata,
        KitError::CapabilityUnsupported(_) | KitError::Unsupported(_) => {
            ErrorCategory::ClusterVersionMismatch
        }
        KitError::QueryConflict { .. } => ErrorCategory::ClusterVersionMismatch,
        KitError::QueryFailed { .. }
        | KitError::RemoteProtocol { .. }
        | KitError::SerializationFailed { .. }
        | KitError::Transport { .. }
        | KitError::Storage(_) => ErrorCategory::ReplicaUnavailable,
    }
}

// ── JNI helpers ───────────────────────────────────────────────────────────

/// Convert a JNI JString to a Rust String.
fn jstring_to_string(env: &mut JNIEnv, s: JString) -> String {
    match env.get_string(&s) {
        Ok(js) => js.to_str().unwrap_or("").to_owned(),
        Err(_) => String::new(),
    }
}

/// Throw a Java exception with the given class and message. The class should
/// be a fully-qualified JVM class name using slashes (e.g. "com/visorcraft/
/// mongreldb/QueryException").
fn throw_java(env: &mut JNIEnv, class: &str, message: &str) {
    let _ = env.throw_new(class, message);
}

/// Throw `QueryException(message, category, categoryCode)` when the class is
/// available; fall back to the single-string constructor / `throw_new`.
fn throw_query_exception(env: &mut JNIEnv, message: &str, category: ErrorCategory) {
    const CLASS: &str = "com/visorcraft/mongreldb/QueryException";
    let ok = (|| -> jni::errors::Result<()> {
        let class = env.find_class(CLASS)?;
        let msg = env.new_string(message)?;
        let cat = env.new_string(category.name())?;
        let obj = env.new_object(
            class,
            "(Ljava/lang/String;Ljava/lang/String;I)V",
            &[
                JValue::Object(&JObject::from(msg)),
                JValue::Object(&JObject::from(cat)),
                JValue::Int(category.code() as i32),
            ],
        )?;
        env.throw(obj)?;
        Ok(())
    })();
    if ok.is_err() {
        // Class missing or constructor unavailable: message-only fallback.
        throw_java(env, CLASS, message);
    }
}

/// Map a KitError to a Java exception and throw it. Surfaces the FND-007
/// taxonomy on `QueryException.category` / `categoryCode`.
fn throw_kit_error(env: &mut JNIEnv, e: &mongreldb_kit::KitError) {
    throw_query_exception(env, &format!("{e}"), kit_error_category(e));
}

/// SAFETY: cast a jlong handle back to the JniDatabase wrapper.
unsafe fn handle_to_db(handle: jlong) -> Option<&'static JniDatabase> {
    if handle == 0 {
        return None;
    }
    Some(&*(handle as *const JniDatabase))
}

/// SAFETY: create a handle from a JniDatabase.
fn db_to_handle(db: JniDatabase) -> jlong {
    Box::into_raw(Box::new(db)) as jlong
}

// ── JNI exported functions ────────────────────────────────────────────────
//
// All follow the signature: extern "system" fn(JNIEnv, JClass, ...) -> ret
// The `system` calling convention matches the JNI ABI on each platform.

#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeBuildInfo(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let json = serde_json::json!({
        "artifact_version": env!("CARGO_PKG_VERSION"),
        "engine_version": env!("CARGO_PKG_VERSION"),
        "query_version": env!("CARGO_PKG_VERSION"),
        "kit_version": env!("CARGO_PKG_VERSION"),
        "mongreldb_git_sha": env!("MONGRELDB_GIT_SHA"),
        "kit_git_sha": env!("MONGRELDB_KIT_GIT_SHA"),
    })
    .to_string();
    env.new_string(json)
        .map(|value| value.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// Opens an existing Kit database. Java: `native long open(String path)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeOpen(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    match KitDatabase::open(Path::new(&path_str)) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Creates a fresh Kit database with a JSON schema.
/// Java: `native long create(String path, String schemaJson)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeCreate(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    schema_json: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let schema_str = jstring_to_string(&mut env, schema_json);
    let schema: mongreldb_kit::Schema = match serde_json::from_str(&schema_str) {
        Ok(s) => s,
        Err(e) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                &format!("failed to parse schema JSON: {e}"),
            );
            return 0;
        }
    };
    match KitDatabase::create(Path::new(&path_str), schema) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Open encrypted Kit database. Java: `native long nativeOpenEncrypted(String path, String passphrase)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeOpenEncrypted(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    passphrase: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let pass = jstring_to_string(&mut env, passphrase);
    match KitDatabase::open_encrypted(Path::new(&path_str), &pass) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Create encrypted Kit database.
/// Java: `native long nativeCreateEncrypted(String path, String schemaJson, String passphrase)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeCreateEncrypted(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    schema_json: JString,
    passphrase: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let schema_str = jstring_to_string(&mut env, schema_json);
    let pass = jstring_to_string(&mut env, passphrase);
    let schema: mongreldb_kit::Schema = match serde_json::from_str(&schema_str) {
        Ok(s) => s,
        Err(e) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                &format!("failed to parse schema JSON: {e}"),
            );
            return 0;
        }
    };
    match KitDatabase::create_encrypted(Path::new(&path_str), schema, &pass) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Open with credentials. Java: `native long nativeOpenWithCredentials(String path, String user, String password)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeOpenWithCredentials(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    user: JString,
    password: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let user_str = jstring_to_string(&mut env, user);
    let pass_str = jstring_to_string(&mut env, password);
    match KitDatabase::open_with_credentials(Path::new(&path_str), &user_str, &pass_str) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Create with credentials.
/// Java: `native long nativeCreateWithCredentials(String path, String schemaJson, String user, String password)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeCreateWithCredentials(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    schema_json: JString,
    user: JString,
    password: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let schema_str = jstring_to_string(&mut env, schema_json);
    let user_str = jstring_to_string(&mut env, user);
    let pass_str = jstring_to_string(&mut env, password);
    let schema: mongreldb_kit::Schema = match serde_json::from_str(&schema_str) {
        Ok(s) => s,
        Err(e) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                &format!("failed to parse schema JSON: {e}"),
            );
            return 0;
        }
    };
    match KitDatabase::create_with_credentials(Path::new(&path_str), schema, &user_str, &pass_str) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Open encrypted + credentials.
/// Java: `native long nativeOpenEncryptedWithCredentials(String path, String passphrase, String user, String password)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeOpenEncryptedWithCredentials(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    passphrase: JString,
    user: JString,
    password: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let pass = jstring_to_string(&mut env, passphrase);
    let user_str = jstring_to_string(&mut env, user);
    let pass_str = jstring_to_string(&mut env, password);
    match KitDatabase::open_encrypted_with_credentials(
        Path::new(&path_str),
        &pass,
        &user_str,
        &pass_str,
    ) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Create encrypted + credentials.
/// Java: `native long nativeCreateEncryptedWithCredentials(String path, String schemaJson, String passphrase, String user, String password)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeCreateEncryptedWithCredentials(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    schema_json: JString,
    passphrase: JString,
    user: JString,
    password: JString,
) -> jlong {
    let path_str = jstring_to_string(&mut env, path);
    let schema_str = jstring_to_string(&mut env, schema_json);
    let pass = jstring_to_string(&mut env, passphrase);
    let user_str = jstring_to_string(&mut env, user);
    let pass_str = jstring_to_string(&mut env, password);
    let schema: mongreldb_kit::Schema = match serde_json::from_str(&schema_str) {
        Ok(s) => s,
        Err(e) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                &format!("failed to parse schema JSON: {e}"),
            );
            return 0;
        }
    };
    match KitDatabase::create_encrypted_with_credentials(
        Path::new(&path_str),
        schema,
        &pass,
        &user_str,
        &pass_str,
    ) {
        Ok(db) => db_to_handle(JniDatabase {
            db: Rc::new(RefCell::new(db)),
        }),
        Err(e) => {
            throw_kit_error(&mut env, &e);
            0
        }
    }
}

/// Closes and frees the database handle. Java: `native void close(long handle)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if handle != 0 {
        // SAFETY: the JVM guarantees the handle was produced by open/create
        // and will not be reused after close.
        unsafe {
            drop(Box::from_raw(handle as *mut JniDatabase));
        }
    }
}

/// Runs SQL and returns a JSON array of row objects.
/// Java: `native String sqlRows(long handle, String sql)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeSqlRows(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    sql: JString,
) -> jstring {
    let db = match unsafe { handle_to_db(handle) } {
        Some(d) => d,
        None => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                "database handle is null",
            );
            return std::ptr::null_mut();
        }
    };
    let sql_str = jstring_to_string(&mut env, sql);

    let rows = match db.db.borrow().sql_rows(&sql_str) {
        Ok(r) => r,
        Err(e) => {
            throw_kit_error(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let json = match serde_json::to_string(&rows) {
        Ok(s) => s,
        Err(e) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                &format!("JSON serialization failed: {e}"),
            );
            return std::ptr::null_mut();
        }
    };

    env.new_string(json)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// Runs SQL and returns Arrow IPC file bytes.
/// Java: `native byte[] sqlArrow(long handle, String sql)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeSqlArrow(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    sql: JString,
) -> jbyteArray {
    let db = match unsafe { handle_to_db(handle) } {
        Some(d) => d,
        None => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                "database handle is null",
            );
            return std::ptr::null_mut();
        }
    };
    let sql_str = jstring_to_string(&mut env, sql);

    let ipc = match db.db.borrow().sql_arrow(&sql_str) {
        Ok(bytes) => bytes,
        Err(e) => {
            throw_kit_error(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    env.byte_array_from_slice(&ipc)
        .map(|a| a.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// Runs the Kit migration runner.
/// Java: `native void migrate(long handle, String migrationsJson)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeMigrate(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    migrations_json: JString,
) {
    let db = match unsafe { handle_to_db(handle) } {
        Some(d) => d,
        None => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                "database handle is null",
            );
            return;
        }
    };
    let json_str = jstring_to_string(&mut env, migrations_json);

    let migrations: Vec<Migration> = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(e) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                &format!("failed to parse migrations JSON: {e}"),
            );
            return;
        }
    };

    let result = db.db.try_borrow_mut();
    let mut guard = match result {
        Ok(g) => g,
        Err(_) => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                "database is in use (borrow conflict)",
            );
            return;
        }
    };

    if let Err(e) = mongreldb_kit::migrate(&mut guard, &migrations) {
        throw_kit_error(&mut env, &e);
    }
}

/// Reads applied migrations as a JSON array.
/// Java: `native String appliedMigrations(long handle)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeAppliedMigrations(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    let db = match unsafe { handle_to_db(handle) } {
        Some(d) => d,
        None => {
            throw_java(
                &mut env,
                "com/visorcraft/mongreldb/QueryException",
                "database handle is null",
            );
            return std::ptr::null_mut();
        }
    };

    let migrations = match db.db.borrow().applied_migrations() {
        Ok(m) => m,
        Err(e) => {
            throw_kit_error(&mut env, &e);
            return std::ptr::null_mut();
        }
    };

    let json = serde_json::to_string(&migrations).unwrap_or_else(|_| "[]".to_string());
    env.new_string(json)
        .map(|s| s.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// Rebuild the SQL session after schema changes.
/// Java: `native void refreshSqlSession(long handle)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeRefreshSqlSession(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    let db = match unsafe { handle_to_db(handle) } {
        Some(d) => d,
        None => return,
    };
    if let Err(e) = db.db.borrow().refresh_sql_session() {
        throw_kit_error(&mut env, &e);
    }
}

/// Runs a SELECT query via the Kit query builder.
/// Java: `native String querySelect(long handle, String queryJson)`.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQuerySelect(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "select")
}

/// Runs a JOIN query.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQueryJoin(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "join")
}

/// Runs an AGGREGATE query.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQueryAggregate(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "aggregate")
}

/// Runs an INSERT query.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQueryInsert(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "insert")
}

/// Runs an UPDATE query.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQueryUpdate(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "update")
}

/// Runs an UPSERT query.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQueryUpsert(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "upsert")
}

/// Runs a DELETE query.
#[no_mangle]
pub extern "system" fn Java_com_visorcraft_mongreldb_native_1mode_NativeDB_nativeQueryDelete(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
) -> jstring {
    query_dispatch(&mut env, handle, query_json, "delete")
}

/// Dispatch a query builder call. Each variant parses the JSON into the
/// appropriate kit-core AST type, runs it in a short-lived transaction, and
/// returns the result as a JSON string.
fn query_dispatch(env: &mut JNIEnv, handle: jlong, query_json: JString, kind: &str) -> jstring {
    let db = match unsafe { handle_to_db(handle) } {
        Some(d) => d,
        None => {
            throw_java(
                env,
                "com/visorcraft/mongreldb/QueryException",
                "database handle is null",
            );
            return std::ptr::null_mut();
        }
    };
    let json_str = jstring_to_string(env, query_json);

    use mongreldb_kit_core::{
        AggregateQuery, Delete, Insert, JoinQuery, Query, Select, Update, Upsert,
    };

    let result_json =
        db.db
            .borrow()
            .transaction(0, |txn| -> Result<String, mongreldb_kit::KitError> {
                match kind {
                    "select" => {
                        let q: Select = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let rows = txn.select(&Query::Select(q.clone()))?;
                        let maps: Vec<_> = rows.iter().map(|r| &r.values).collect();
                        serde_json::to_string(&maps)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    "join" => {
                        let q: JoinQuery = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let rows = txn.join(&q.clone())?;
                        serde_json::to_string(&rows)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    "aggregate" => {
                        let q: AggregateQuery = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let rows = txn.aggregate(&q.clone())?;
                        let maps: Vec<_> = rows.iter().map(|r| &r.values).collect();
                        serde_json::to_string(&maps)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    "insert" => {
                        let q: Insert = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let vals = txn.execute(&Query::Insert(q.clone()))?;
                        serde_json::to_string(&vals)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    "update" => {
                        let q: Update = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let vals = txn.execute(&Query::Update(q.clone()))?;
                        serde_json::to_string(&vals)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    "upsert" => {
                        let q: Upsert = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let vals = txn.execute(&Query::Upsert(q.clone()))?;
                        serde_json::to_string(&vals)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    "delete" => {
                        let q: Delete = serde_json::from_str(&json_str)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))?;
                        let vals = txn.execute(&Query::Delete(q.clone()))?;
                        serde_json::to_string(&vals)
                            .map_err(|e| mongreldb_kit::KitError::Storage(e.to_string()))
                    }
                    _ => Err(mongreldb_kit::KitError::Storage(format!(
                        "unknown query kind: {kind}"
                    ))),
                }
            });

    match result_json {
        Ok(json) => env
            .new_string(json)
            .map(|s| s.into_raw())
            .unwrap_or(std::ptr::null_mut()),
        Err(e) => {
            throw_kit_error(env, &e);
            std::ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod taxonomy_tests {
    use super::kit_error_category;
    use mongreldb_kit::KitError;
    use mongreldb_types::errors::ErrorCategory;

    #[test]
    fn permission_denied_and_resource_map_to_stable_codes() {
        assert_eq!(
            kit_error_category(&KitError::PermissionDenied("alice lacks Admin".into())),
            ErrorCategory::PermissionDenied
        );
        assert_eq!(
            kit_error_category(&KitError::PermissionDenied("x".into())).code(),
            20
        );
        assert_eq!(
            kit_error_category(&KitError::PermissionDenied("x".into())).name(),
            "permission denied"
        );

        assert_eq!(
            kit_error_category(&KitError::DatabaseLocked("busy".into())),
            ErrorCategory::ResourceExhausted
        );
        assert_eq!(
            kit_error_category(&KitError::DatabaseLocked("busy".into())).code(),
            18
        );

        assert_eq!(
            kit_error_category(&KitError::Integrity("missing table".into())),
            ErrorCategory::StaleMetadata
        );
        assert_eq!(
            kit_error_category(&KitError::Integrity("missing table".into())).code(),
            3
        );
    }

    #[test]
    fn auth_conflict_and_outcome_map() {
        assert_eq!(
            kit_error_category(&KitError::InvalidCredentials("bob".into())),
            ErrorCategory::Unauthenticated
        );
        assert_eq!(
            kit_error_category(&KitError::Conflict("ww".into())),
            ErrorCategory::TransactionConflict
        );
        let unknown = KitError::OutcomeUnknown {
            query_id: "q".into(),
            message: "fsync".into(),
            metadata: Box::default(),
        };
        assert_eq!(
            kit_error_category(&unknown),
            ErrorCategory::CommitOutcomeUnknown
        );
        assert_eq!(kit_error_category(&unknown).code(), 12);
    }

    #[test]
    fn taxonomy_codes_are_1_through_20_never_reused() {
        let codes: Vec<u32> = ErrorCategory::ALL.iter().map(|c| c.code()).collect();
        assert_eq!(codes, (1..=20).collect::<Vec<_>>());
        for category in ErrorCategory::ALL {
            assert_eq!(ErrorCategory::from_code(category.code()), Some(category));
            assert_eq!(category.name(), category.to_string());
        }
    }
}
