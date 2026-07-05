//! Encrypted page-stats envelope (run format v2): per-page min/max for
//! encrypted columns travel AES-256-GCM-encrypted under the run DEK and are
//! overlaid at open, restoring zone-map page pruning without leaking bounds
//! into the cleartext column directory. Covers: range-query result parity
//! with plaintext tables across multiple pages, absence of plaintext bounds
//! in the file bytes, and tamper rejection of the envelope.
#![cfg(feature = "encryption")]

use mongreldb_core::query::{Condition, Query};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{read_header, Table, Value};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

// Distinctive byte strings that become the tag column's global min/max — the
// exact values a leaky stats implementation would write into the directory.
const CANARY_MIN: &[u8] = b"aaa-leak-canary-minimum-bound";
const CANARY_MAX: &[u8] = b"zzz-leak-canary-maximum-bound";

/// Rows spanning several 65 536-row pages so pruning has something to skip.
const N: i64 = 200_000;

fn schema() -> Schema {
    Schema {
        schema_id: 71,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "cost".into(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty(),
            },
            ColumnDef {
                id: 3,
                name: "tag".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn rows() -> Vec<Vec<(u16, Value)>> {
    (0..N)
        .map(|i| {
            let tag: Vec<u8> = if i == 0 {
                CANARY_MIN.to_vec()
            } else if i == N - 1 {
                CANARY_MAX.to_vec()
            } else {
                format!("tag-{i:07}").into_bytes()
            };
            vec![
                (1, Value::Int64(i)),
                (2, Value::Float64(i as f64)),
                (3, Value::Bytes(tag)),
            ]
        })
        .collect()
}

fn run_file(dir: &Path) -> PathBuf {
    let runs = dir.join("_runs");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&runs)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().map(|e| e == "sr").unwrap_or(false))
        .collect();
    assert_eq!(files.len(), 1, "expected exactly one run in {runs:?}");
    files.pop().unwrap()
}

fn range_query(db: &mut Table, lo: f64, hi: f64) -> Vec<i64> {
    let q = Query::new().and(Condition::RangeF64 {
        column_id: 2,
        lo,
        lo_inclusive: true,
        hi,
        hi_inclusive: true,
    });
    let mut ids: Vec<i64> = db
        .query(&q)
        .unwrap()
        .iter()
        .map(|r| match r.columns.get(&1) {
            Some(Value::Int64(v)) => *v,
            other => panic!("expected Int64 id, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Multi-page encrypted range queries return exactly what the plaintext table
/// returns — for a page-0-only filter (the pruned case), a page-boundary-
/// crossing filter, and a filter over the last page.
#[test]
fn encrypted_range_results_match_plaintext() {
    let enc_dir = tempdir().unwrap();
    let plain_dir = tempdir().unwrap();
    let mut enc = Table::create_encrypted(enc_dir.path(), schema(), 1, "pw").unwrap();
    let mut plain = Table::create(plain_dir.path(), schema(), 1).unwrap();
    enc.bulk_load(rows()).unwrap();
    plain.bulk_load(rows()).unwrap();

    for (lo, hi) in [
        (0.0, 100.0),                        // page 0 only — prunable tail
        (65_000.0, 66_000.0),                // crosses the page-0/page-1 boundary
        ((N - 50) as f64, (N + 100) as f64), // last page, hi past the max
    ] {
        let got = range_query(&mut enc, lo, hi);
        let want = range_query(&mut plain, lo, hi);
        assert_eq!(got, want, "encrypted != plaintext for [{lo}, {hi}]");
        assert!(!want.is_empty(), "test range [{lo}, {hi}] must match rows");
    }
}

/// The run file must not contain the tag column's plaintext min/max anywhere:
/// the bounds now travel only inside the encrypted stats envelope. (The v2
/// header must also record that envelope.)
#[test]
fn encrypted_stats_envelope_leaks_no_plaintext_bounds() {
    let dir = tempdir().unwrap();
    let mut db = Table::create_encrypted(dir.path(), schema(), 1, "pw").unwrap();
    db.bulk_load(rows()).unwrap();

    let path = run_file(dir.path());
    let header = read_header(&path).unwrap();
    assert_ne!(
        header.encrypted_stats_offset, 0,
        "encrypted run must carry the encrypted stats envelope"
    );
    assert_ne!(header.encrypted_stats_len, 0);

    let bytes = std::fs::read(&path).unwrap();
    for canary in [CANARY_MIN, CANARY_MAX] {
        assert!(
            !bytes.windows(canary.len()).any(|w| w == canary),
            "plaintext page bound leaked into the run file: {:?}",
            String::from_utf8_lossy(canary)
        );
    }
}

/// Corrupting the stats envelope (with the unkeyed footer checksum fixed up,
/// as an attacker without the key can do) must be rejected at open by the
/// envelope's AES-256-GCM authentication.
#[test]
fn tampered_stats_envelope_is_rejected() {
    let dir = tempdir().unwrap();
    {
        let mut db = Table::create_encrypted(dir.path(), schema(), 1, "pw").unwrap();
        db.bulk_load(rows()).unwrap();
    }
    let path = run_file(dir.path());
    let header = read_header(&path).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();

    // Flip one byte inside the envelope, then recompute the (unkeyed) footer
    // checksum so only the AEAD stands between the attacker and the stats.
    let target = header.encrypted_stats_offset as usize + 4;
    bytes[target] ^= 0xFF;
    let foot = header.footer_offset as usize;
    let checksum = {
        use sha2::{Digest, Sha256};
        Sha256::digest(&bytes[..foot])
    };
    bytes[foot + 16..foot + 48].copy_from_slice(&checksum);
    std::fs::write(&path, &bytes).unwrap();

    let err = Table::open_encrypted(dir.path(), "pw");
    assert!(
        err.is_err(),
        "open must reject a run whose stats envelope fails authentication"
    );
}
