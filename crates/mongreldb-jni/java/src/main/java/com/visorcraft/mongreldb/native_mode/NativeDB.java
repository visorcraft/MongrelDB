package com.visorcraft.mongreldb.native_mode;

import com.visorcraft.mongreldb.native_package.NativeLoader;

/**
 * In-process embedded MongrelDB database via JNI. No daemon, no HTTP overhead -
 * the engine runs directly in the JVM via {@code libmongreldb_jni}.
 *
 * <p>This class wraps the Kit layer (schema model, migrations, query builder,
 * SQL). The database handle is a {@code long} owned by this instance and freed
 * on {@link #close()}. The handle is NOT thread-safe; create one
 * {@code NativeDB} per thread if you need concurrency.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * String schemaJson = "{\"tables\":[{\"id\":1,\"name\":\"users\",...}]}";
 * try (NativeDB db = NativeDB.create("/path/to/dbdir", schemaJson)) {
 *     db.sqlRows("INSERT INTO users (id, name) VALUES (1, 'alice')");
 *     String rows = db.sqlRows("SELECT id, name FROM users");
 *     byte[] arrow = db.sqlArrow("SELECT * FROM users");
 *     db.migrate(migrationsJson);
 * }
 * }</pre>
 */
public class NativeDB implements AutoCloseable {

    private long handle;
    private boolean closed;

    private static volatile boolean nativeLoaded = false;

    /**
     * Ensures the native library is loaded. Called lazily by the constructors
     * so that merely referencing the class (e.g. in a test guard) does not
     * crash with {@link UnsatisfiedLinkError}.
     */
    private static void ensureNative() {
        if (!nativeLoaded) {
            synchronized (NativeDB.class) {
                if (!nativeLoaded) {
                    NativeLoader.ensureLoaded();
                    nativeLoaded = true;
                }
            }
        }
    }

    /** Opens an existing database (creates if missing). */
    public NativeDB(String path) {
        ensureNative();
        this.handle = nativeOpen(path);
        if (this.handle == 0) {
            throw new RuntimeException("Failed to open database: " + path);
        }
    }

    /** Creates a fresh database with a JSON schema. */
    public static NativeDB create(String path, String schemaJson) {
        ensureNative();
        long h = nativeCreate(path, schemaJson);
        if (h == 0) {
            throw new RuntimeException("Failed to create database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    /** Opens an AES-256-GCM encrypted database with a passphrase. */
    public static NativeDB openEncrypted(String path, String passphrase) {
        ensureNative();
        long h = nativeOpenEncrypted(path, passphrase);
        if (h == 0) {
            throw new RuntimeException("Failed to open encrypted database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    /** Creates an AES-256-GCM encrypted database with a passphrase. */
    public static NativeDB createEncrypted(String path, String schemaJson, String passphrase) {
        ensureNative();
        long h = nativeCreateEncrypted(path, schemaJson, passphrase);
        if (h == 0) {
            throw new RuntimeException("Failed to create encrypted database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    /** Opens a database with storage-layer username/password credentials. */
    public static NativeDB openWithCredentials(String path, String username, String password) {
        ensureNative();
        long h = nativeOpenWithCredentials(path, username, password);
        if (h == 0) {
            throw new RuntimeException("Failed to open credentialed database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    /** Creates a credentialed database (require_auth + admin user). */
    public static NativeDB createWithCredentials(
            String path, String schemaJson, String username, String password) {
        ensureNative();
        long h = nativeCreateWithCredentials(path, schemaJson, username, password);
        if (h == 0) {
            throw new RuntimeException("Failed to create credentialed database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    /** Opens encrypted + credentialed database (passphrase and username/password). */
    public static NativeDB openEncryptedWithCredentials(
            String path, String passphrase, String username, String password) {
        ensureNative();
        long h = nativeOpenEncryptedWithCredentials(path, passphrase, username, password);
        if (h == 0) {
            throw new RuntimeException("Failed to open encrypted credentialed database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    /** Creates encrypted + credentialed database (passphrase and admin user). */
    public static NativeDB createEncryptedWithCredentials(
            String path, String schemaJson, String passphrase, String username, String password) {
        ensureNative();
        long h = nativeCreateEncryptedWithCredentials(
                path, schemaJson, passphrase, username, password);
        if (h == 0) {
            throw new RuntimeException("Failed to create encrypted credentialed database: " + path);
        }
        NativeDB db = new NativeDB();
        db.handle = h;
        return db;
    }

    private NativeDB() {}

    /**
     * Checks whether the native library can be loaded. Safe to call at any
     * time; returns {@code false} instead of throwing if the library is
     * missing. Tests use this to decide whether to skip.
     */
    public static boolean nativeAvailable() {
        try {
            ensureNative();
            return true;
        } catch (UnsatisfiedLinkError e) {
            return false;
        }
    }

    /** Returns exact runtime artifact, engine, query, Kit, and Git identities as JSON. */
    public static String buildInfo() {
        ensureNative();
        return nativeBuildInfo();
    }

    // ── Instance methods (delegate to static JNI with the handle) ──────────

    /** Runs SQL, returns a JSON array of row objects. DDL/DML returns "[]". */
    public String sqlRows(String sql) {
        checkOpen();
        return nativeSqlRows(handle, sql);
    }

    /** Runs SQL, returns Arrow IPC file bytes. DDL/DML returns empty array. */
    public byte[] sqlArrow(String sql) {
        checkOpen();
        return nativeSqlArrow(handle, sql);
    }

    /** Runs the Kit migration runner with a JSON array of Migration objects. */
    public void migrate(String migrationsJson) {
        checkOpen();
        nativeMigrate(handle, migrationsJson);
    }

    /** Reads applied migrations as a JSON array. */
    public String appliedMigrations() {
        checkOpen();
        return nativeAppliedMigrations(handle);
    }

    /** Rebuild the SQL session after schema changes. */
    public void refreshSqlSession() {
        checkOpen();
        nativeRefreshSqlSession(handle);
    }

    /** Runs a SELECT query (JSON Select AST), returns JSON rows. */
    public String querySelect(String queryJson) {
        checkOpen();
        return nativeQuerySelect(handle, queryJson);
    }

    /** Runs a JOIN query (JSON JoinQuery AST), returns JSON rows. */
    public String queryJoin(String queryJson) {
        checkOpen();
        return nativeQueryJoin(handle, queryJson);
    }

    /** Runs an AGGREGATE query, returns JSON rows. */
    public String queryAggregate(String queryJson) {
        checkOpen();
        return nativeQueryAggregate(handle, queryJson);
    }

    /** Runs an INSERT query, returns JSON returning values. */
    public String queryInsert(String queryJson) {
        checkOpen();
        return nativeQueryInsert(handle, queryJson);
    }

    /** Runs an UPDATE query, returns JSON returning values. */
    public String queryUpdate(String queryJson) {
        checkOpen();
        return nativeQueryUpdate(handle, queryJson);
    }

    /** Runs an UPSERT query, returns JSON returning values. */
    public String queryUpsert(String queryJson) {
        checkOpen();
        return nativeQueryUpsert(handle, queryJson);
    }

    /** Runs a DELETE query, returns JSON returning values. */
    public String queryDelete(String queryJson) {
        checkOpen();
        return nativeQueryDelete(handle, queryJson);
    }

    @Override
    public void close() {
        if (!closed && handle != 0) {
            nativeClose(handle);
            handle = 0;
            closed = true;
        }
    }

    private void checkOpen() {
        if (closed || handle == 0) {
            throw new IllegalStateException("NativeDB is closed");
        }
    }

    // ── Static native methods (the actual JNI bridge) ──────────────────────

    private static native long nativeOpen(String path);
    private static native String nativeBuildInfo();
    private static native long nativeCreate(String path, String schemaJson);
    private static native long nativeOpenEncrypted(String path, String passphrase);
    private static native long nativeCreateEncrypted(
            String path, String schemaJson, String passphrase);
    private static native long nativeOpenWithCredentials(
            String path, String username, String password);
    private static native long nativeCreateWithCredentials(
            String path, String schemaJson, String username, String password);
    private static native long nativeOpenEncryptedWithCredentials(
            String path, String passphrase, String username, String password);
    private static native long nativeCreateEncryptedWithCredentials(
            String path, String schemaJson, String passphrase, String username, String password);
    private static native void nativeClose(long handle);
    private static native String nativeSqlRows(long handle, String sql);
    private static native byte[] nativeSqlArrow(long handle, String sql);
    private static native void nativeMigrate(long handle, String migrationsJson);
    private static native String nativeAppliedMigrations(long handle);
    private static native void nativeRefreshSqlSession(long handle);
    private static native String nativeQuerySelect(long handle, String queryJson);
    private static native String nativeQueryJoin(long handle, String queryJson);
    private static native String nativeQueryAggregate(long handle, String queryJson);
    private static native String nativeQueryInsert(long handle, String queryJson);
    private static native String nativeQueryUpdate(long handle, String queryJson);
    private static native String nativeQueryUpsert(long handle, String queryJson);
    private static native String nativeQueryDelete(long handle, String queryJson);
}
