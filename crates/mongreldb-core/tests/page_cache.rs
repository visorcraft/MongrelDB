//! Phase 9.2 — shared, persistent, MVCC content-addressed page cache.
//!
//! The cache sits under `RunReader::read_page`: all readers share one
//! `Arc<Mutex<PageCache>>`, the parallel `read_page_shared` path probes it
//! non-blockingly, and an optional `_cache/` backing dir survives restart.

use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "v".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "v_lr".into(),
            column_id: 2,
            kind: IndexKind::LearnedRange,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
    }
}

#[test]
fn shared_cache_serves_reads_across_readers() {
    let dir = tempdir().unwrap();
    let mut db = Table::create(dir.path(), schema(), 1).unwrap();
    // Enough rows to span multiple pages so the cache has real work to do.
    for i in 0..200_000i64 {
        db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i * 2))])
            .unwrap();
    }
    db.flush().unwrap();

    // First scan warms the cache.
    let snap = db.snapshot();
    let out = db.visible_columns_native(snap, None).unwrap();
    let n = out
        .iter()
        .find(|(c, _)| *c == 1)
        .map(|(_, c)| c.len())
        .unwrap();
    assert_eq!(n, 200_000);

    // A second scan over the same (shared) cache must still be correct — the
    // cache transparently serves the same pages to every reader path.
    let snap2 = db.snapshot();
    let out2 = db.visible_columns_native(snap2, None).unwrap();
    let n2 = out2
        .iter()
        .find(|(c, _)| *c == 1)
        .map(|(_, c)| c.len())
        .unwrap();
    assert_eq!(n, n2);
}

#[test]
fn persistent_cache_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so run pages warm the page cache
        for i in 0..100_000i64 {
            db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i / 7))])
                .unwrap();
        }
        db.flush().unwrap();
        // Warm the cache so pages spill to `_cache/`.
        let snap = db.snapshot();
        let _ = db.visible_columns_native(snap, None).unwrap();
        db.page_cache_flush();
    }
    // The backing dir must hold spilled pages.
    let cache_dir = dir.path().join("_cache");
    assert!(cache_dir.exists(), "_cache dir should exist");
    let spilled = std::fs::read_dir(&cache_dir).unwrap().count();
    assert!(spilled > 0, "expected spilled page files, got {spilled}");

    // Reopen: the persistent cache is reloaded; reads are still correct.
    let db = Table::open(dir.path()).unwrap();
    assert_eq!(db.count(), 100_000);
    let snap = db.snapshot();
    let out = db.visible_columns_native(snap, None).unwrap();
    let n = out
        .iter()
        .find(|(c, _)| *c == 1)
        .map(|(_, c)| c.len())
        .unwrap();
    assert_eq!(n, 100_000);
}

// Uses the encryption-only `Table::create_encrypted` API, so gate it behind the
// `encryption` feature (the rest of this file builds without it).
#[cfg(feature = "encryption")]
#[test]
fn cache_does_not_break_encrypted_reads() {
    // The cache stores raw ciphertext bytes and decrypts AFTER the lookup, so an
    // encrypted table must round-trip correctly through the shared cache.
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "passphrase").unwrap();
        for i in 0..5_000i64 {
            db.put(vec![(1, Value::Int64(i)), (2, Value::Int64(i + 1))])
                .unwrap();
        }
        db.flush().unwrap();
        // Two scans: the second is served partly from the cache (ciphertext),
        // re-decrypted on hit.
        let s1 = db.snapshot();
        let o1 = db.visible_columns_native(s1, None).unwrap();
        let s2 = db.snapshot();
        let o2 = db.visible_columns_native(s2, None).unwrap();
        let n1 = o1
            .iter()
            .find(|(c, _)| *c == 1)
            .map(|(_, c)| c.len())
            .unwrap();
        let n2 = o2
            .iter()
            .find(|(c, _)| *c == 1)
            .map(|(_, c)| c.len())
            .unwrap();
        assert_eq!(n1, 5_000);
        assert_eq!(n2, 5_000);
    }
    let db = Table::open_encrypted(dir.path(), "passphrase").unwrap();
    assert_eq!(db.count(), 5_000);
}
