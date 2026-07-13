//! P1.3 — DB-wide catalog checkpoint (encrypted + authenticated, dir-fsync).

use mongreldb_core::{
    catalog::{self, Catalog, CatalogEntry, TableState},
    schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId},
};
use tempfile::tempdir;

fn sample_schema() -> Schema {
    Schema {
        schema_id: 7,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "pk".into(),
            column_id: 1,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn sample_catalog() -> Catalog {
    Catalog {
        db_epoch: 7,
        next_table_id: 3,
        next_segment_no: 4,
        tables: vec![CatalogEntry {
            table_id: 1,
            name: "orders".into(),
            schema: sample_schema(),
            state: TableState::Live,
            created_epoch: 2,
        }],
        procedures: Vec::new(),
        triggers: Vec::new(),
        external_tables: Vec::new(),
        materialized_views: Vec::new(),
        security: Default::default(),
        users: Vec::new(),
        roles: Vec::new(),
        next_user_id: 0,
        require_auth: false,
    }
}

#[test]
fn catalog_roundtrips_plaintext_and_dir_fsync() {
    let dir = tempdir().unwrap();
    let cat = sample_catalog();
    catalog::write_atomic(dir.path(), &cat, None).unwrap();
    let got = catalog::read(dir.path(), None).unwrap().unwrap();
    assert_eq!(got.db_epoch, 7);
    assert_eq!(got.next_table_id, 3);
    assert_eq!(got.next_segment_no, 4);
    assert_eq!(got.tables.len(), 1);
    assert_eq!(got.tables[0].name, "orders");
    assert_eq!(got.tables[0].table_id, 1);
    assert!(matches!(got.tables[0].state, TableState::Live));
    assert_eq!(got.tables[0].schema.columns.len(), 2);
}

#[test]
fn catalog_read_returns_none_when_missing() {
    let dir = tempdir().unwrap();
    assert!(catalog::read(dir.path(), None).unwrap().is_none());
}

#[cfg(feature = "encryption")]
#[test]
fn catalog_encrypted_is_authenticated() {
    let dir = tempdir().unwrap();
    let dek = [9u8; 32];
    let cat = sample_catalog();
    catalog::write_atomic(dir.path(), &cat, Some(&dek)).unwrap();
    // roundtrips under the right key
    let got = catalog::read(dir.path(), Some(&dek)).unwrap().unwrap();
    assert_eq!(got.db_epoch, 7);
    // tamper a byte of the file -> read must fail auth (None), not silently parse
    let p = dir.path().join("CATALOG");
    let mut b = std::fs::read(&p).unwrap();
    let n = b.len();
    b[n / 2] ^= 0xFF;
    std::fs::write(&p, b).unwrap();
    assert!(catalog::read(dir.path(), Some(&dek)).unwrap().is_none());
}

#[cfg(feature = "encryption")]
#[test]
fn catalog_encrypted_wrong_key_returns_none() {
    let dir = tempdir().unwrap();
    let dek = [9u8; 32];
    let cat = sample_catalog();
    catalog::write_atomic(dir.path(), &cat, Some(&dek)).unwrap();
    let wrong = [0u8; 32];
    assert!(catalog::read(dir.path(), Some(&wrong)).unwrap().is_none());
}
