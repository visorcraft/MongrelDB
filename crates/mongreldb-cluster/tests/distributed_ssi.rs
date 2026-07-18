//! Stage 3 gate: distributed serializable anomaly suite.
//!
//! Drives the shipped [`mongreldb_cluster::dist_ssi::DistSsiCertifier`] —
//! multi-tablet write-skew and WW conflicts abort; disjoint writes commit.

use mongreldb_cluster::dist_ssi::{DistCertOutcome, DistSsiCertifier};
use mongreldb_types::ids::{TabletId, TransactionId};

fn tid(n: u8) -> TabletId {
    TabletId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}
fn txn(n: u8) -> TransactionId {
    TransactionId::from_bytes({
        let mut b = [0u8; 16];
        b[15] = n;
        b
    })
}

#[test]
fn write_skew_across_two_tablets_is_rejected() {
    let mut cert = DistSsiCertifier::new(32);
    // T1 reads y on tablet 2, writes x on tablet 1.
    cert.observe_read(txn(1), tid(2), b"y");
    cert.observe_write(txn(1), tid(1), b"x");
    assert_eq!(cert.certify(txn(1)), DistCertOutcome::Ok);
    // T2 reads x on tablet 1, writes y on tablet 2 (classic write-skew).
    cert.observe_read(txn(2), tid(1), b"x");
    cert.observe_write(txn(2), tid(2), b"y");
    assert_eq!(
        cert.certify(txn(2)),
        DistCertOutcome::SerializationFailure,
        "distributed serializable must reject multi-tablet write-skew"
    );
}

#[test]
fn same_key_ww_across_prepare_is_rejected() {
    let mut cert = DistSsiCertifier::new(8);
    cert.observe_write(txn(1), tid(3), b"balance");
    assert_eq!(cert.certify(txn(1)), DistCertOutcome::Ok);
    cert.observe_write(txn(2), tid(3), b"balance");
    assert_eq!(cert.certify(txn(2)), DistCertOutcome::SerializationFailure);
}

#[test]
fn partitionable_disjoint_commits() {
    let mut cert = DistSsiCertifier::new(8);
    cert.observe_write(txn(1), tid(1), b"a");
    cert.observe_write(txn(2), tid(2), b"b");
    assert_eq!(cert.certify(txn(1)), DistCertOutcome::Ok);
    assert_eq!(cert.certify(txn(2)), DistCertOutcome::Ok);
}
