//! Distributed serializable certification (Stage 3 gate / ADR-0007).
//!
//! Multi-tablet SSI over participant read/write sets: a transaction may
//! commit only when its read set does not intersect any concurrent
//! committed write set (rw-antidependency) and it does not form a
//! write-skew cycle with a concurrent committed transaction
//! (rw edges both ways across possibly distinct tablets).
//!
//! This is the certification core the gateway/coordinator uses before
//! advertising distributed serializable. Single-node SSI in `mongreldb-core`
//! remains for local txns; this module is the cross-tablet path.

use std::collections::BTreeMap;

use mongreldb_types::ids::{TabletId, TransactionId};
use serde::{Deserialize, Serialize};

/// One key in a tablet's read or write set (opaque partition-key bytes).
pub type KeyBytes = Vec<u8>;

/// Per-tablet key set.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletKeySet {
    /// Keys touched on this tablet.
    pub keys: Vec<KeyBytes>,
}

impl TabletKeySet {
    /// Empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert key if not already present.
    pub fn insert(&mut self, key: impl Into<KeyBytes>) {
        let key = key.into();
        if !self.keys.iter().any(|k| k == &key) {
            self.keys.push(key);
        }
    }

    /// Whether any key is shared with `other`.
    pub fn intersects(&self, other: &Self) -> bool {
        self.keys.iter().any(|k| other.keys.iter().any(|o| o == k))
    }
}

/// Full multi-tablet access set for one transaction.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistAccessSet {
    /// Per-tablet reads.
    pub reads: BTreeMap<TabletId, TabletKeySet>,
    /// Per-tablet writes.
    pub writes: BTreeMap<TabletId, TabletKeySet>,
}

impl DistAccessSet {
    /// Record a read.
    pub fn read(&mut self, tablet: TabletId, key: impl Into<KeyBytes>) {
        self.reads.entry(tablet).or_default().insert(key);
    }

    /// Record a write.
    pub fn write(&mut self, tablet: TabletId, key: impl Into<KeyBytes>) {
        self.writes.entry(tablet).or_default().insert(key);
    }

    /// Whether this write set intersects `other`'s read set on any tablet.
    pub fn writes_intersect_reads(&self, other: &Self) -> bool {
        for (tablet, wset) in &self.writes {
            if let Some(rset) = other.reads.get(tablet) {
                if wset.intersects(rset) {
                    return true;
                }
            }
        }
        false
    }

    /// Whether write sets intersect (WW conflict).
    pub fn writes_intersect_writes(&self, other: &Self) -> bool {
        for (tablet, wset) in &self.writes {
            if let Some(ow) = other.writes.get(tablet) {
                if wset.intersects(ow) {
                    return true;
                }
            }
        }
        false
    }
}

/// Outcome of certification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DistCertOutcome {
    /// Safe to commit under serializable.
    Ok,
    /// Concurrent WW or dangerous rw structure.
    SerializationFailure,
}

/// In-memory multi-tablet SSI certifier (process-local coordinator half).
///
/// Production coordinators feed prepare-time access sets here before the
/// durable Commit decision; tests drive the same type for anomaly suites.
#[derive(Debug, Default)]
pub struct DistSsiCertifier {
    /// In-flight access sets.
    inflight: BTreeMap<TransactionId, DistAccessSet>,
    /// Recently committed access sets still in the certification window.
    committed: Vec<(TransactionId, DistAccessSet)>,
    /// Max committed retained (bounded).
    window: usize,
}

impl DistSsiCertifier {
    /// New certifier with certification window size.
    pub fn new(window: usize) -> Self {
        Self {
            inflight: BTreeMap::new(),
            committed: Vec::new(),
            window: window.max(1),
        }
    }

    /// Replace the access set for an in-flight txn (idempotent prepare).
    pub fn track(&mut self, txn: TransactionId, access: DistAccessSet) {
        self.inflight.insert(txn, access);
    }

    /// Merge a read into the in-flight set.
    pub fn observe_read(&mut self, txn: TransactionId, tablet: TabletId, key: impl Into<KeyBytes>) {
        self.inflight.entry(txn).or_default().read(tablet, key);
    }

    /// Merge a write into the in-flight set.
    pub fn observe_write(
        &mut self,
        txn: TransactionId,
        tablet: TabletId,
        key: impl Into<KeyBytes>,
    ) {
        self.inflight.entry(txn).or_default().write(tablet, key);
    }

    /// Certify `txn` against the committed window. On success the access set
    /// moves from inflight to committed.
    pub fn certify(&mut self, txn: TransactionId) -> DistCertOutcome {
        let Some(mine) = self.inflight.remove(&txn) else {
            // Empty access set is trivially serializable.
            return DistCertOutcome::Ok;
        };
        for (_other_id, other) in &self.committed {
            // WW conflict.
            if mine.writes_intersect_writes(other) {
                // Put back so a retry can re-observe; actually abort leaves it out.
                return DistCertOutcome::SerializationFailure;
            }
            // rw-antidependency: their writes hit our reads.
            if other.writes_intersect_reads(&mine) {
                // Dangerous if we also write something they read (write-skew)
                // OR always abort on rw-conflict for strict SSI certification
                // of concurrent commits. Spec serializable: abort on
                // uncommitted-write-skew patterns and WW.
                if mine.writes_intersect_reads(other) {
                    return DistCertOutcome::SerializationFailure;
                }
                // Single-direction rw-edge: also fail for strict serializable
                // anomaly suite (prevents G2-item when the inverse edge is
                // established by our commit ordering).
                return DistCertOutcome::SerializationFailure;
            }
            // Inverse rw: our writes hit their reads while they write elsewhere
            // that we read — covered when the other commits first then we
            // check writes_intersect_reads above on the swapped pair.
            if mine.writes_intersect_reads(other) && other.writes_intersect_reads(&mine) {
                return DistCertOutcome::SerializationFailure;
            }
        }
        self.committed.push((txn, mine));
        if self.committed.len() > self.window {
            let drop = self.committed.len() - self.window;
            self.committed.drain(0..drop);
        }
        DistCertOutcome::Ok
    }

    /// Abort / forget an in-flight txn without committing.
    pub fn abort(&mut self, txn: TransactionId) {
        self.inflight.remove(&txn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn multi_tablet_write_skew_aborts() {
        // T1: read y@tablet2, write x@tablet1
        // T2: read x@tablet1, write y@tablet2
        let mut cert = DistSsiCertifier::new(32);
        let t1 = txn(1);
        let t2 = txn(2);
        cert.observe_read(t1, tid(2), b"y");
        cert.observe_write(t1, tid(1), b"x");
        assert_eq!(cert.certify(t1), DistCertOutcome::Ok);

        cert.observe_read(t2, tid(1), b"x");
        cert.observe_write(t2, tid(2), b"y");
        assert_eq!(cert.certify(t2), DistCertOutcome::SerializationFailure);
    }

    #[test]
    fn multi_tablet_ww_aborts() {
        let mut cert = DistSsiCertifier::new(8);
        cert.observe_write(txn(1), tid(1), b"k");
        assert_eq!(cert.certify(txn(1)), DistCertOutcome::Ok);
        cert.observe_write(txn(2), tid(1), b"k");
        assert_eq!(cert.certify(txn(2)), DistCertOutcome::SerializationFailure);
    }

    #[test]
    fn disjoint_writes_commit() {
        let mut cert = DistSsiCertifier::new(8);
        cert.observe_write(txn(1), tid(1), b"a");
        assert_eq!(cert.certify(txn(1)), DistCertOutcome::Ok);
        cert.observe_write(txn(2), tid(2), b"b");
        assert_eq!(cert.certify(txn(2)), DistCertOutcome::Ok);
    }
}
