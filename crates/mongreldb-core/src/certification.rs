//! Production certification harness inventory (spec section 14.6, Stage 5F).
//!
//! Enumerates the required test classes and points at the in-repo suites that
//! satisfy them. This module is not a test runner — it is a durable checklist
//! the Stage 5 gate and operators can query.

use serde::{Deserialize, Serialize};

/// One certification class from spec §14.6.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CertificationClass {
    /// Class name.
    pub name: &'static str,
    /// Spec category.
    pub category: CertificationCategory,
    /// In-repo evidence paths (tests, workflows, benches).
    pub evidence: &'static [&'static str],
    /// Whether a smoke entry point exists in-tree today.
    pub smoke_ready: bool,
}

/// Top-level certification category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CertificationCategory {
    /// Unit/property tests.
    UnitProperty,
    /// Fuzz targets.
    Fuzz,
    /// Crash-at-hook matrix.
    CrashHooks,
    /// Consensus simulation.
    ConsensusSim,
    /// Reference-model transaction checker.
    TxnChecker,
    /// Chaos cluster.
    Chaos,
    /// Performance suites with provenance.
    Perf,
}

/// The full inventory of certification classes.
pub fn certification_inventory() -> Vec<CertificationClass> {
    vec![
        CertificationClass {
            name: "encoding_round_trips",
            category: CertificationCategory::UnitProperty,
            evidence: &[
                "crates/mongreldb-log/tests/envelope.rs",
                "crates/mongreldb-types",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "mvcc_visibility",
            category: CertificationCategory::UnitProperty,
            evidence: &["crates/mongreldb-core/tests/isolation.rs"],
            smoke_ready: true,
        },
        CertificationClass {
            name: "timestamp_ordering",
            category: CertificationCategory::UnitProperty,
            evidence: &[
                "crates/mongreldb-types/src/hlc.rs",
                "crates/mongreldb-consensus/tests/hlc_monotonicity.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "routing",
            category: CertificationCategory::UnitProperty,
            evidence: &[
                "crates/mongreldb-cluster/src/tablet.rs",
                "crates/mongreldb-cluster/src/routing.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "transaction_state_machine",
            category: CertificationCategory::UnitProperty,
            evidence: &[
                "crates/mongreldb-core/tests/txn_*.rs",
                "crates/mongreldb-cluster/src/dist_txn.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "catalog_mutations",
            category: CertificationCategory::UnitProperty,
            evidence: &["crates/mongreldb-core/tests/catalog_commands.rs"],
            smoke_ready: true,
        },
        CertificationClass {
            name: "protocol_decode_fuzz",
            category: CertificationCategory::Fuzz,
            evidence: &["crates/mongreldb-protocol/src/envelope.rs"],
            smoke_ready: true,
        },
        CertificationClass {
            name: "wal_log_decode",
            category: CertificationCategory::Fuzz,
            evidence: &["crates/mongreldb-core/tests/fault_injection.rs"],
            smoke_ready: true,
        },
        CertificationClass {
            name: "snapshot_decode",
            category: CertificationCategory::Fuzz,
            evidence: &["crates/mongreldb-consensus/src/state_machine.rs"],
            smoke_ready: true,
        },
        CertificationClass {
            name: "crash_at_hooks",
            category: CertificationCategory::CrashHooks,
            evidence: &[
                "crates/mongreldb-fault",
                "crates/mongreldb-cluster/src/split.rs",
                "crates/mongreldb-cluster/src/merge.rs",
                "crates/mongreldb-cluster/src/cluster_backup.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "consensus_sim",
            category: CertificationCategory::ConsensusSim,
            evidence: &[
                "crates/mongreldb-sim",
                "crates/mongreldb-consensus/tests/chaos.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "reference_model_txn_checker",
            category: CertificationCategory::TxnChecker,
            evidence: &[
                "crates/mongreldb-core/tests/isolation.rs",
                "crates/mongreldb-cluster/src/dist_txn.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "chaos_cluster",
            category: CertificationCategory::Chaos,
            evidence: &[
                "crates/mongreldb-consensus/tests/chaos.rs",
                "crates/mongreldb-cluster/tests/runtime.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "perf_oltp_mixed",
            category: CertificationCategory::Perf,
            evidence: &[
                "BENCHMARKS.md",
                "crates/mongreldb-core/tests/qualification.rs",
            ],
            smoke_ready: true,
        },
        CertificationClass {
            name: "perf_ai_rag",
            category: CertificationCategory::Perf,
            evidence: &[
                "crates/mongreldb-core/examples/ai_retrieval_bench.rs",
                "BENCHMARKS.md",
            ],
            smoke_ready: true,
        },
    ]
}

/// Smoke: every class is named and at least one evidence path is non-empty.
pub fn inventory_smoke() -> Result<(), String> {
    let inv = certification_inventory();
    if inv.is_empty() {
        return Err("empty certification inventory".into());
    }
    for c in &inv {
        if c.name.is_empty() {
            return Err("unnamed certification class".into());
        }
        if c.evidence.is_empty() {
            return Err(format!("{} has no evidence paths", c.name));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_covers_required_categories() {
        inventory_smoke().unwrap();
        let inv = certification_inventory();
        for cat in [
            CertificationCategory::UnitProperty,
            CertificationCategory::Fuzz,
            CertificationCategory::CrashHooks,
            CertificationCategory::ConsensusSim,
            CertificationCategory::TxnChecker,
            CertificationCategory::Chaos,
            CertificationCategory::Perf,
        ] {
            assert!(
                inv.iter().any(|c| c.category == cat),
                "missing category {cat:?}"
            );
        }
        assert!(inv.iter().all(|c| c.smoke_ready));
    }
}
