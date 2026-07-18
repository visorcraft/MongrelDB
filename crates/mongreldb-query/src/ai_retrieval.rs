//! Distributed AI retrieval (spec section 13.4, Stage 4D).
//!
//! Per-tablet retrievers apply RLS **before** local top-k, return bounded
//! candidates with raw scores, and the coordinator merges with deterministic
//! RRF (or configured fusion). Tie-break: final score desc, tablet id asc,
//! RowId asc. Adaptive ANN candidate counts with global work budget,
//! candidate ceiling, memory, and deadline.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use mongreldb_core::RowId;
use mongreldb_types::ids::TabletId;
use serde::{Deserialize, Serialize};

/// One candidate from a tablet-local retriever (post-RLS).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalCandidate {
    /// Source tablet.
    pub tablet_id: TabletId,
    /// Row identity.
    pub row_id: RowId,
    /// Raw local score (higher is better).
    pub score: f64,
    /// Local rank (1-based) after RLS filtering.
    pub local_rank: u32,
    /// Whether this row was visible after RLS (must be true when emitted).
    pub rls_visible: bool,
}

/// Fusion method for coordinator merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FusionMethod {
    /// Reciprocal rank fusion with constant `k` (default 60).
    Rrf {
        /// RRF constant k.
        k: u32,
    },
    /// Max of raw scores (deterministic with tie-breaks).
    MaxScore,
}

impl Default for FusionMethod {
    fn default() -> Self {
        Self::Rrf { k: 60 }
    }
}

/// Global work budget for a distributed AI request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiWorkBudget {
    /// Maximum candidates retained globally after merge.
    pub candidate_ceiling: usize,
    /// Maximum total local candidates fetched across tablets.
    pub max_local_candidates: usize,
    /// Memory reservation bytes (advisory for the governor).
    pub memory_bytes: u64,
    /// Wall-clock deadline remaining in milliseconds.
    pub deadline_ms: u64,
}

impl Default for AiWorkBudget {
    fn default() -> Self {
        Self {
            candidate_ceiling: 100,
            max_local_candidates: 1_000,
            memory_bytes: 64 * 1024 * 1024,
            deadline_ms: 5_000,
        }
    }
}

/// Adaptive per-tablet candidate count.
///
/// `ceil(global_k * overfetch_factor / active_tablet_count)`.
pub fn adaptive_local_k(global_k: usize, overfetch_factor: f64, active_tablets: usize) -> usize {
    let tablets = active_tablets.max(1) as f64;
    let raw = (global_k as f64) * overfetch_factor / tablets;
    raw.ceil().max(1.0) as usize
}

/// Errors of distributed AI retrieval.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AiRetrievalError {
    /// A tablet returned an RLS-hidden row as a candidate (hygiene violation).
    #[error("RLS hygiene violation: tablet {tablet_id} emitted hidden row {row_id}")]
    RlsHygiene {
        /// Offending tablet.
        tablet_id: TabletId,
        /// Hidden row.
        row_id: u64,
    },
    /// Work budget exceeded.
    #[error("AI work budget exceeded: {0}")]
    BudgetExceeded(String),
}

/// One merged result row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergedCandidate {
    /// Tablet of origin (lowest tablet id if fused across; for RRF we keep
    /// the tablet of the best local rank contribution for tie-break).
    pub tablet_id: TabletId,
    /// Row id.
    pub row_id: RowId,
    /// Fused final score.
    pub final_score: f64,
    /// Best raw local score observed.
    pub raw_score: f64,
}

#[derive(Debug, Clone)]
struct HeapKey {
    final_score: f64,
    tablet_id: TabletId,
    row_id: RowId,
}

impl PartialEq for HeapKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for HeapKey {}
impl PartialOrd for HeapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Max-heap: higher score first; then lower tablet id; then lower row id.
        match self
            .final_score
            .partial_cmp(&other.final_score)
            .unwrap_or(Ordering::Equal)
        {
            Ordering::Equal => other
                .tablet_id
                .cmp(&self.tablet_id)
                .then_with(|| other.row_id.cmp(&self.row_id)),
            ord => ord,
        }
    }
}

/// Deterministic coordinator merge (spec §13.4).
///
/// RLS hygiene: any candidate with `rls_visible == false` is a hard error
/// (hidden rows must never influence ranking).
pub fn merge_candidates(
    locals: &[LocalCandidate],
    method: FusionMethod,
    budget: &AiWorkBudget,
) -> Result<Vec<MergedCandidate>, AiRetrievalError> {
    if locals.len() > budget.max_local_candidates {
        return Err(AiRetrievalError::BudgetExceeded(format!(
            "{} local candidates > max {}",
            locals.len(),
            budget.max_local_candidates
        )));
    }
    for c in locals {
        if !c.rls_visible {
            return Err(AiRetrievalError::RlsHygiene {
                tablet_id: c.tablet_id,
                row_id: c.row_id.0,
            });
        }
    }

    // Group by (tablet, row) — a row should appear once per tablet.
    let mut by_key: HashMap<(TabletId, RowId), LocalCandidate> = HashMap::new();
    for c in locals {
        by_key
            .entry((c.tablet_id, c.row_id))
            .and_modify(|existing| {
                if c.score > existing.score {
                    *existing = c.clone();
                }
            })
            .or_insert_with(|| c.clone());
    }

    let fused: Vec<MergedCandidate> = match method {
        FusionMethod::Rrf { k } => {
            // RRF score = sum 1/(k + rank) across appearances (here one per tablet).
            by_key
                .into_values()
                .map(|c| {
                    let rrf = 1.0 / (f64::from(k) + f64::from(c.local_rank));
                    MergedCandidate {
                        tablet_id: c.tablet_id,
                        row_id: c.row_id,
                        final_score: rrf,
                        raw_score: c.score,
                    }
                })
                .collect()
        }
        FusionMethod::MaxScore => by_key
            .into_values()
            .map(|c| MergedCandidate {
                tablet_id: c.tablet_id,
                row_id: c.row_id,
                final_score: c.score,
                raw_score: c.score,
            })
            .collect(),
    };

    // Sort with deterministic tie-breaks, take ceiling.
    let mut heap: BinaryHeap<HeapKey> = BinaryHeap::new();
    let mut map: HashMap<(TabletId, RowId), MergedCandidate> = HashMap::new();
    for m in fused {
        let key = (m.tablet_id, m.row_id);
        heap.push(HeapKey {
            final_score: m.final_score,
            tablet_id: m.tablet_id,
            row_id: m.row_id,
        });
        map.insert(key, m);
    }

    let mut out = Vec::with_capacity(budget.candidate_ceiling.min(map.len()));
    while out.len() < budget.candidate_ceiling {
        let Some(hk) = heap.pop() else {
            break;
        };
        if let Some(m) = map.remove(&(hk.tablet_id, hk.row_id)) {
            out.push(m);
        }
    }
    Ok(out)
}

/// Audit metadata returned with AI/analytics results (spec §13.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiConsistencyAudit {
    /// Requested read timestamp (string form of HLC).
    pub read_ts: String,
    /// Replica applied timestamp.
    pub replica_applied_ts: String,
    /// Measured staleness in microseconds (0 if caught up).
    pub staleness_micros: u64,
    /// Index applied_through timestamp.
    pub index_applied_ts: String,
    /// Model / preprocessing version.
    pub model_version: Option<String>,
    /// Preprocessing version.
    pub preprocessing_version: Option<String>,
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

    fn cand(tablet: u8, row: u64, score: f64, rank: u32) -> LocalCandidate {
        LocalCandidate {
            tablet_id: tid(tablet),
            row_id: RowId(row),
            score,
            local_rank: rank,
            rls_visible: true,
        }
    }

    #[test]
    fn merge_is_deterministic() {
        let locals = vec![
            cand(2, 10, 0.9, 1),
            cand(1, 20, 0.8, 1),
            cand(2, 30, 0.7, 2),
            cand(1, 10, 0.95, 2),
        ];
        let budget = AiWorkBudget {
            candidate_ceiling: 10,
            ..AiWorkBudget::default()
        };
        let a = merge_candidates(&locals, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        let b = merge_candidates(&locals, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        assert_eq!(a, b);
        // Score desc: rank-1 entries outrank rank-2 for same k.
        assert!(a[0].final_score >= a[1].final_score);
    }

    #[test]
    fn rls_hidden_row_fails_closed() {
        let mut c = cand(1, 1, 1.0, 1);
        c.rls_visible = false;
        let err =
            merge_candidates(&[c], FusionMethod::default(), &AiWorkBudget::default()).unwrap_err();
        assert!(matches!(err, AiRetrievalError::RlsHygiene { .. }));
    }

    #[test]
    fn adaptive_local_k_scales() {
        assert_eq!(adaptive_local_k(10, 2.0, 5), 4); // ceil(4.0)
        assert_eq!(adaptive_local_k(10, 2.0, 1), 20);
        assert_eq!(adaptive_local_k(10, 2.0, 0), 20); // treat 0 tablets as 1
    }

    #[test]
    fn tie_break_tablet_then_row() {
        // Equal RRF ranks → lower tablet id first among equal scores.
        let locals = vec![cand(2, 5, 1.0, 1), cand(1, 9, 1.0, 1)];
        let out = merge_candidates(
            &locals,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget {
                candidate_ceiling: 2,
                ..AiWorkBudget::default()
            },
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        // Same rank → same RRF score; tablet 1 before tablet 2.
        assert_eq!(out[0].tablet_id, tid(1));
        assert_eq!(out[1].tablet_id, tid(2));
    }
}
