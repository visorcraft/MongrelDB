//! Phase 10.1 — §7 key-hierarchy integration tests.
//!
//! Verifies the per-table Argon2id+HKDF KEK, the persisted salt, the per-file
//! wrapped DEK stored in each run's Encryption Descriptor, and deterministic
//! per-page nonces — entirely through the public `Table` API plus the public
//! `read_header`.

#![cfg(feature = "encryption")]

use mongreldb_core::{
    schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId},
    sorted_run::{read_column_dir, read_header},
    Condition, EncryptionDescriptor, Query, Table, Value,
};
use std::io::{Read, Seek, SeekFrom};
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 7,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "secret".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// A schema whose `label` column (id 2) is ENCRYPTED_INDEXABLE with a Bitmap
/// equality index — so the bitmap stores HMAC tokens (Phase 10.2).
fn schema_indexable_eq() -> Schema {
    Schema {
        schema_id: 8,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::ENCRYPTED_INDEXABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "label_eq".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
            options: Default::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

/// Read the bincode Encryption Descriptor body (4-byte len prefix + body) the
/// writer embedded at `header.encryption_descriptor_offset`.
fn read_descriptor(path: &std::path::Path) -> EncryptionDescriptor {
    let header = read_header(path).unwrap();
    assert_ne!(
        header.encryption_descriptor_offset, 0,
        "encrypted run must carry an encryption descriptor"
    );
    let mut f = std::fs::File::open(path).unwrap();
    f.seek(SeekFrom::Start(header.encryption_descriptor_offset))
        .unwrap();
    let mut len_buf = [0u8; 4];
    f.read_exact(&mut len_buf).unwrap();
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf).unwrap();
    bincode::deserialize(&buf).unwrap()
}

fn run_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = std::fs::read_dir(dir.join("_runs"))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sr"))
        .collect();
    files.sort();
    files
}

#[test]
fn empty_encrypted_table_reopens_without_any_write() {
    // An encrypted table created with NO writes/flush must reopen: the manifest
    // written at create time has to be encrypted + authenticated so the reopen's
    // manifest read can authenticate it. A plaintext create-time manifest would
    // make the table permanently unopenable.
    let dir = tempdir().unwrap();
    {
        let _db = Table::create_encrypted(dir.path(), schema(), 1, "pw").unwrap();
        // no put, no flush, no commit
    }
    let db = Table::open_encrypted(dir.path(), "pw").unwrap();
    assert_eq!(db.count(), 0);
}

#[test]
fn salt_file_persisted_at_create() {
    let dir = tempdir().unwrap();
    let _ = Table::create_encrypted(dir.path(), schema(), 1, "passphrase").unwrap();
    let salt = std::fs::read(dir.path().join("_meta").join("keys")).unwrap();
    assert_eq!(salt.len(), mongreldb_core::encryption::SALT_LEN);
}

#[test]
fn each_run_carries_a_wrapped_dek() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "passphrase").unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so the run file carries a descriptor
        let _ = db
            .put(vec![(1, Value::Int64(1)), (2, Value::Bytes(b"a".to_vec()))])
            .unwrap();
        db.flush().unwrap();
    }
    let runs = run_files(dir.path());
    assert_eq!(runs.len(), 1);
    let desc = read_descriptor(&runs[0]);
    assert_eq!(desc.algo, mongreldb_core::encryption::ALGO_AES_GCM);
    // wrapped_dek = AES-256-GCM(KEK) over a 32-byte DEK: 32 + 16-byte tag.
    assert_eq!(
        desc.wrapped_dek.len(),
        mongreldb_core::encryption::DEK_LEN + 16
    );
    assert!(
        desc.nonce_prefix[..8].iter().any(|&b| b != 0),
        "nonce prefix high bytes must be random (nonzero)"
    );
}

#[test]
fn per_file_dek_differs_between_runs() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "passphrase").unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so the run file carries a descriptor
                                           // Run 1.
        let _ = db
            .put(vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"same".to_vec())),
            ])
            .unwrap();
        db.flush().unwrap();
        // Run 2 (distinct memtable → distinct run).
        let _ = db
            .put(vec![
                (1, Value::Int64(2)),
                (2, Value::Bytes(b"same".to_vec())),
            ])
            .unwrap();
        db.flush().unwrap();
    }
    let runs = run_files(dir.path());
    assert_eq!(runs.len(), 2, "expected two runs");
    let d1 = read_descriptor(&runs[0]);
    let d2 = read_descriptor(&runs[1]);
    assert_ne!(
        d1.wrapped_dek, d2.wrapped_dek,
        "each run must wrap a distinct random DEK"
    );
    assert_ne!(
        d1.nonce_prefix, d2.nonce_prefix,
        "each run must have a distinct random nonce prefix"
    );
}

#[test]
fn correct_passphrase_round_trips_across_reopen() {
    let dir = tempdir().unwrap();
    let id = {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "the right one").unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so decryption runs against a real run
        let id = db
            .put(vec![
                (1, Value::Int64(42)),
                (2, Value::Bytes(b"topsecret".to_vec())),
            ])
            .unwrap();
        db.flush().unwrap();
        id
    };
    let db = Table::open_encrypted(dir.path(), "the right one").unwrap();
    let row = db.get(id, db.snapshot()).unwrap();
    assert!(matches!(row.columns.get(&2), Some(Value::Bytes(b)) if b == b"topsecret"));
}

#[test]
fn wrong_passphrase_fails_to_decrypt() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "right").unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so a wrong passphrase must unwrap a DEK
        let _ = db
            .put(vec![(1, Value::Int64(1)), (2, Value::Bytes(b"x".to_vec()))])
            .unwrap();
        db.flush().unwrap();
    }
    // Deriving the (wrong) KEK succeeds; the failure surfaces when a run's DEK
    // must be unwrapped — either at open (index rebuild) or on first read.
    let opened = Table::open_encrypted(dir.path(), "wrong");
    let read_fails = match opened {
        Ok(db) => db.visible_rows(db.snapshot()).is_err(),
        Err(_) => true,
    };
    assert!(
        read_fails,
        "a wrong passphrase must not be able to decrypt run pages"
    );
}

#[test]
fn plaintext_table_has_no_salt_or_descriptor() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create(dir.path(), schema(), 1).unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so a plaintext run exists to inspect
        let _ = db
            .put(vec![(1, Value::Int64(1)), (2, Value::Bytes(b"s".to_vec()))])
            .unwrap();
        db.flush().unwrap();
    }
    assert!(
        !dir.path().join("_meta").join("keys").exists(),
        "plaintext tables must not persist an encryption salt"
    );
    let runs = run_files(dir.path());
    let header = read_header(&runs[0]).unwrap();
    assert_eq!(
        header.encryption_descriptor_offset, 0,
        "plaintext runs must not carry an encryption descriptor"
    );
}

#[test]
fn indexable_equality_query_served_via_tokenized_bitmap() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema_indexable_eq(), 1, "pass").unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so the run carries a column-key descriptor
        for i in 0..40u64 {
            let label = if i % 2 == 0 {
                b"red".to_vec()
            } else {
                b"blue".to_vec()
            };
            db.put(vec![(1, Value::Int64(i as i64)), (2, Value::Bytes(label))])
                .unwrap();
        }
        db.flush().unwrap();
    }
    // Reopen with the correct passphrase: the bitmap index (persisted via the
    // global checkpoint, or rebuilt from runs) holds HMAC tokens, and the
    // BitmapEq literal is tokenized the same way — so the lookup is served
    // without decrypting the stored page payloads.
    let mut db = Table::open_encrypted(dir.path(), "pass").unwrap();
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"red".to_vec(),
    });
    let red = db.query(&q).unwrap();
    assert_eq!(red.len(), 20, "exactly the even rows are 'red'");
    for r in &red {
        assert!(
            matches!(r.columns.get(&2), Some(Value::Bytes(b)) if b == b"red"),
            "tokenized equality query must return exactly-matching rows"
        );
    }
    // The other bucket.
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"blue".to_vec(),
    });
    assert_eq!(db.query(&q).unwrap().len(), 20);

    // A value that was never inserted yields nothing.
    let q = Query::new().and(Condition::BitmapEq {
        column_id: 2,
        value: b"green".to_vec(),
    });
    assert!(db.query(&q).unwrap().is_empty());
}

#[test]
fn indexable_descriptor_records_column_key() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema_indexable_eq(), 1, "pass").unwrap();
        db.set_mutable_run_spill_bytes(1); // spill so the run carries a column-key descriptor
        let _ = db
            .put(vec![
                (1, Value::Int64(1)),
                (2, Value::Bytes(b"red".to_vec())),
            ])
            .unwrap();
        db.flush().unwrap();
    }
    let runs = run_files(dir.path());
    let desc = read_descriptor(&runs[0]);
    assert_eq!(
        desc.column_descriptors.len(),
        1,
        "one ENCRYPTED_INDEXABLE column"
    );
    let cd = &desc.column_descriptors[0];
    assert_eq!(cd.column_id, 2);
    assert_eq!(cd.scheme, mongreldb_core::encryption::SCHEME_HMAC_EQ);
    // wrapped column key = AES-256-GCM tag + 32-byte key = 48 bytes.
    assert_eq!(
        cd.wrapped_column_key.len(),
        mongreldb_core::encryption::DEK_LEN + 16
    );
}

/// Regression (peer review): the column directory is serialized in cleartext, so
/// per-page `min`/`max` must NOT carry plaintext values for encrypted columns —
/// otherwise an at-rest attacker reads literal values straight out of the `.sr`
/// file without the key. Asserts a distinctive plaintext canary never appears in
/// the raw run bytes (it would, as the min==max of its page, before the fix).
#[test]
fn encrypted_run_directory_does_not_leak_plaintext_minmax() {
    const CANARY: &[u8] = b"CANARY-SECRET-must-not-hit-disk-in-cleartext";
    let dir = tempdir().unwrap();
    let id = {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "pw").unwrap();
        db.set_mutable_run_spill_bytes(1);
        let id = db
            .put(vec![
                (1, Value::Int64(7)),
                (2, Value::Bytes(CANARY.to_vec())),
            ])
            .unwrap();
        db.flush().unwrap();
        id
    };
    let runs = run_files(dir.path());
    assert_eq!(runs.len(), 1);
    let raw = std::fs::read(&runs[0]).unwrap();
    assert!(
        raw.windows(CANARY.len()).all(|w| w != CANARY),
        "plaintext canary leaked into the encrypted run file (min/max in the \
         cleartext directory?)"
    );
    // And the value still round-trips with the key (decrypt path intact).
    let db = Table::open_encrypted(dir.path(), "pw").unwrap();
    let row = db.get(id, db.snapshot()).unwrap();
    assert!(matches!(row.columns.get(&2), Some(Value::Bytes(b)) if b == CANARY));
}

/// Regression (peer review): with min/max suppressed for encrypted columns, the
/// range resolvers must fall back to a full decrypt-and-scan instead of treating
/// a missing stat as "all-null" and skipping the page (which would silently drop
/// every matching row). A range query over an encrypted Int64 column must still
/// return exactly the matching rows.
#[test]
fn encrypted_int64_range_query_returns_correct_rows() {
    let dir = tempdir().unwrap();
    let mut db = Table::create_encrypted(dir.path(), schema(), 1, "pw").unwrap();
    db.set_mutable_run_spill_bytes(1);
    for i in 1..=100i64 {
        db.put(vec![
            (1, Value::Int64(i)),
            (2, Value::Bytes(format!("v{i}").into_bytes())),
        ])
        .unwrap();
    }
    db.flush().unwrap();
    let got = db
        .query(&Query::new().and(Condition::Range {
            column_id: 1,
            lo: 10,
            hi: 20,
        }))
        .unwrap()
        .len();
    assert_eq!(got, 11, "encrypted range scan must not skip matching pages");
}

/// Regression (peer review, run-metadata MAC): the cleartext run directory/header
/// drive page decoding but were guarded only by an UNKEYED SHA-256 an attacker can
/// recompute. Each encrypted run now carries a KEK-derived HMAC over
/// header+dir+descriptor, appended after the footer. Corrupting that tag (the only
/// bytes nothing else reads) must reject reads — a non-vacuous proof the MAC is
/// both written and enforced. The same verification covers any tamper of the
/// header/dir/descriptor it authenticates.
#[test]
fn run_metadata_mac_is_written_and_enforced() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "pw").unwrap();
        db.set_mutable_run_spill_bytes(1);
        for i in 1..=20i64 {
            db.put(vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("v{i}").into_bytes())),
            ])
            .unwrap();
        }
        db.flush().unwrap();
    }
    let runs = run_files(dir.path());
    let path = &runs[0];
    let header = read_header(path).unwrap();
    let mut bytes = std::fs::read(path).unwrap();

    // The 32-byte MAC tag sits right after the 48-byte footer; the writer must
    // have appended it.
    let tag_off = header.footer_offset as usize + 48;
    assert!(
        bytes.len() >= tag_off + 32,
        "encrypted run must carry a 32-byte metadata MAC tag"
    );
    // Corrupt the tag. It is past the SHA-256-checksummed region, so the unkeyed
    // checks still pass — only the keyed MAC can reject.
    bytes[tag_off] ^= 0xFF;
    std::fs::write(path, &bytes).unwrap();
    assert!(
        read_header(path).is_ok(),
        "tag is past the checksummed region"
    );
    assert!(
        read_column_dir(path, &header).is_ok(),
        "directory still parses"
    );

    // Open may load the still-valid checkpoint without touching the run; the query
    // forces a run read, which must be rejected by the MAC.
    let opened = Table::open_encrypted(dir.path(), "pw");
    let rejected = match opened {
        Err(_) => true,
        Ok(mut db) => db
            .query(&Query::new().and(Condition::Range {
                column_id: 1,
                lo: 1,
                hi: 20,
            }))
            .is_err(),
    };
    assert!(
        rejected,
        "a corrupted run-metadata MAC must reject run reads"
    );
}

/// Regression (peer review, checkpoint encryption): the persisted index
/// checkpoint embeds index keys / PGM segments derived from user data. For an
/// encrypted table it must be encrypted at rest — not begin with the cleartext
/// `MONGRIDX` magic — and still round-trip on reopen with the key.
#[test]
fn index_checkpoint_is_encrypted_at_rest() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema_indexable_eq(), 1, "pw").unwrap();
        db.set_mutable_run_spill_bytes(1);
        for i in 1..=20i64 {
            db.put(vec![
                (1, Value::Int64(i)),
                (2, Value::Bytes(format!("label{i}").into_bytes())),
            ])
            .unwrap();
        }
        db.flush().unwrap();
    }
    let idx = dir.path().join("_idx").join("global.idx");
    assert!(idx.exists(), "checkpoint should exist after flush");
    let bytes = std::fs::read(&idx).unwrap();
    assert!(
        bytes.len() < 8 || &bytes[..8] != b"MONGRIDX",
        "encrypted table's index checkpoint leaked the cleartext magic"
    );
    // Reopen loads the (encrypted) checkpoint and an indexed lookup still works.
    let mut db = Table::open_encrypted(dir.path(), "pw").unwrap();
    let n = db
        .query(&Query::new().and(Condition::BitmapEq {
            column_id: 2,
            value: b"label7".to_vec(),
        }))
        .unwrap()
        .len();
    assert_eq!(
        n, 1,
        "indexed equality lookup must work after encrypted reopen"
    );
}
