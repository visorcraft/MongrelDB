//! Distributed AI retrieval (spec section 13.4, Stage 4D).
//!
//! Per-tablet retrievers apply RLS **before** local top-k, return bounded
//! candidates with raw scores, and the coordinator merges with deterministic
//! RRF (or configured fusion). Tie-break: final score desc, tablet id asc,
//! RowId asc. Adaptive ANN candidate counts with global work budget,
//! candidate ceiling, memory, and deadline. [`RemoteAiTransport`] fans typed
//! searches over authenticated node-internal RPC routes; workers validate the
//! forwarded authorization envelope and apply RLS before local top-k.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bincode::Options;
use futures::stream::{FuturesUnordered, StreamExt};
use mongreldb_core::query::{Rerank, Retriever, SearchRequest};
use mongreldb_core::{CancellationReason, ExecutionControl, RowId, Value};
use mongreldb_types::ids::{QueryId, TabletId};
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

/// Global work budget for a distributed AI request (P0.8-T4 parent budget).
///
/// One coordinator budget covers tablet RPCs, local candidates, retained
/// contributions, adaptive refill rounds, payload cells, exact gathers,
/// rerank dimensions, and serialization.
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
    /// Maximum tablet RPCs issued by one coordinator fan-out (including refill).
    #[serde(default = "default_max_tablet_rpcs")]
    pub max_tablet_rpcs: usize,
    /// Maximum retained hybrid contributions across all tablets/retrievers.
    #[serde(default = "default_max_retained_contributions")]
    pub max_retained_contributions: usize,
    /// Maximum adaptive hybrid refill rounds.
    #[serde(default = "default_max_refill_rounds")]
    pub max_refill_rounds: usize,
    /// Maximum projected payload cells retained from tablet hits.
    #[serde(default = "default_max_payload_cells")]
    pub max_payload_cells: usize,
    /// Maximum exact-vector gather rows for global rerank.
    #[serde(default = "default_max_exact_gathers")]
    pub max_exact_gathers: usize,
    /// Maximum dimensions (or tokens) for exact rerank payloads.
    #[serde(default = "default_max_rerank_dimensions")]
    pub max_rerank_dimensions: usize,
    /// Maximum serialized request/response bytes retained by the coordinator.
    #[serde(default = "default_max_serialization_bytes")]
    pub max_serialization_bytes: usize,
    /// When set, every tablet hit must report this model generation (fail-closed).
    #[serde(default)]
    pub expected_model_generation: Option<u64>,
}

fn default_max_tablet_rpcs() -> usize {
    256
}
fn default_max_retained_contributions() -> usize {
    4_096
}
fn default_max_refill_rounds() -> usize {
    64
}
fn default_max_payload_cells() -> usize {
    65_536
}
fn default_max_exact_gathers() -> usize {
    1_024
}
fn default_max_rerank_dimensions() -> usize {
    4_096
}
fn default_max_serialization_bytes() -> usize {
    16 * 1024 * 1024
}

impl Default for AiWorkBudget {
    fn default() -> Self {
        Self {
            candidate_ceiling: 100,
            max_local_candidates: 1_000,
            memory_bytes: 64 * 1024 * 1024,
            deadline_ms: 5_000,
            max_tablet_rpcs: default_max_tablet_rpcs(),
            max_retained_contributions: default_max_retained_contributions(),
            max_refill_rounds: default_max_refill_rounds(),
            max_payload_cells: default_max_payload_cells(),
            max_exact_gathers: default_max_exact_gathers(),
            max_rerank_dimensions: default_max_rerank_dimensions(),
            max_serialization_bytes: default_max_serialization_bytes(),
            expected_model_generation: None,
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
    /// The request was cancelled locally or remotely.
    #[error("distributed AI request cancelled: {0:?}")]
    Cancelled(CancellationReason),
    /// Authenticated node-to-node transport failed.
    #[error("distributed AI transport failed: {0}")]
    Transport(String),
    /// A peer violated the versioned distributed-AI protocol.
    #[error("distributed AI protocol error: {0}")]
    Protocol(String),
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

// ---------------------------------------------------------------------------
// Global hybrid fusion (P0.8): per-retriever contributions + coordinator RRF
// ---------------------------------------------------------------------------

/// One tablet-local hit from a single named retriever (post-RLS).
///
/// Multiple contributions for the same `(row_id, retriever_id)` may arrive
/// from different tablets; the coordinator keeps one contribution per named
/// retriever per global row before fusing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalRetrieverContribution {
    /// Source tablet.
    pub tablet_id: TabletId,
    /// Row identity (globally unique within the table).
    pub row_id: RowId,
    /// Named retriever identity (`dense`, `sparse`, `minhash`, …).
    pub retriever_id: String,
    /// Optional retriever kind tag for protocol diagnostics.
    pub retriever_kind: Option<String>,
    /// Local 1-based rank within this retriever on this tablet (after RLS).
    pub local_rank: u32,
    /// Raw local score (higher is better in the contribution protocol).
    pub raw_score: f64,
    /// Upper bound on the score of any *unseen* local hit for this
    /// retriever after `local_rank` (`None` when the tablet is exhausted for
    /// this retriever).
    pub upper_bound_after: Option<f64>,
    /// Whether this row was visible after RLS (must be true when emitted).
    pub rls_visible: bool,
    /// Retriever weight used by RRF (`weight / (k + rank)`). Defaults to 1.0
    /// when omitted at the merge call site.
    pub weight: f64,
}

impl LocalRetrieverContribution {
    /// Builds a contribution with weight 1.0 and no kind/bound metadata.
    pub fn new(
        tablet_id: TabletId,
        row_id: RowId,
        retriever_id: impl Into<String>,
        local_rank: u32,
        raw_score: f64,
    ) -> Self {
        Self {
            tablet_id,
            row_id,
            retriever_id: retriever_id.into(),
            retriever_kind: None,
            local_rank,
            raw_score,
            upper_bound_after: None,
            rls_visible: true,
            weight: 1.0,
        }
    }
}

/// Per-retriever component of a fused hybrid hit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridComponentScore {
    /// Named retriever.
    pub retriever_id: String,
    /// Optional kind tag.
    pub retriever_kind: Option<String>,
    /// Tablet that contributed this component.
    pub tablet_id: TabletId,
    /// 1-based local rank used for fusion.
    pub rank: u32,
    /// Raw local score.
    pub raw_score: f64,
    /// Contribution to the fused score (`weight / (k + rank)` for RRF).
    pub contribution: f64,
    /// Weight applied for this retriever.
    pub weight: f64,
}

/// One globally fused hybrid candidate with component provenance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridMergedCandidate {
    /// Tie-break tablet (lowest tablet id among the row's contributions).
    pub tablet_id: TabletId,
    /// Row identity.
    pub row_id: RowId,
    /// Fused final score.
    pub final_score: f64,
    /// Best raw local score across components.
    pub raw_score: f64,
    /// Per-retriever component scores (sorted by retriever_id for stability).
    pub components: Vec<HybridComponentScore>,
}

/// Exhaustion / bound metadata for one `(tablet, retriever)` stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetrieverTabletBound {
    /// Tablet of the stream.
    pub tablet_id: TabletId,
    /// Named retriever.
    pub retriever_id: String,
    /// Next 1-based rank that would be assigned to an unseen hit (`None` if
    /// exhausted).
    pub next_rank: Option<u32>,
    /// Upper bound on the raw score of any unseen hit (`None` if exhausted
    /// or unknown).
    pub unseen_score_bound: Option<f64>,
    /// True when the tablet has no further hits for this retriever.
    pub exhausted: bool,
    /// Weight applied to this retriever during fusion.
    pub weight: f64,
}

impl RetrieverTabletBound {
    /// Convenience constructor for an exhausted stream.
    pub fn exhausted(tablet_id: TabletId, retriever_id: impl Into<String>) -> Self {
        Self {
            tablet_id,
            retriever_id: retriever_id.into(),
            next_rank: None,
            unseen_score_bound: None,
            exhausted: true,
            weight: 1.0,
        }
    }

    /// Convenience constructor for a live stream with a next rank bound.
    pub fn open(
        tablet_id: TabletId,
        retriever_id: impl Into<String>,
        next_rank: u32,
        unseen_score_bound: Option<f64>,
        weight: f64,
    ) -> Self {
        Self {
            tablet_id,
            retriever_id: retriever_id.into(),
            next_rank: Some(next_rank.max(1)),
            unseen_score_bound,
            exhausted: false,
            weight,
        }
    }
}

/// Outcome of one global hybrid merge step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridMergeResult {
    /// Winners under the candidate ceiling, best first.
    pub candidates: Vec<HybridMergedCandidate>,
    /// `(tablet, retriever)` streams that still need refill before the top-k
    /// is provably exact under the declared bounds.
    pub refill: Vec<(TabletId, String)>,
}

/// Deterministic global hybrid fusion over per-retriever contributions
/// (P0.8 / audit §11.4).
///
/// Algorithm:
/// 1. Reject RLS-hidden contributions (fail-closed).
/// 2. Deduplicate to one contribution per `(row_id, retriever_id)` — keep the
///    best rank (then higher raw score, then lower tablet id).
/// 3. Group by `row_id`, compute RRF (`Σ weight/(k+rank)`) or max-score fusion,
///    and retain component provenance.
/// 4. Sort by fused score desc, tablet id asc, row id asc; apply the ceiling.
/// 5. Report adaptive-refill targets from `bounds` when provided.
pub fn merge_hybrid_contributions(
    contributions: &[LocalRetrieverContribution],
    method: FusionMethod,
    budget: &AiWorkBudget,
    bounds: &[RetrieverTabletBound],
) -> Result<HybridMergeResult, AiRetrievalError> {
    if contributions.len() > budget.max_local_candidates {
        return Err(AiRetrievalError::BudgetExceeded(format!(
            "{} hybrid contributions > max {}",
            contributions.len(),
            budget.max_local_candidates
        )));
    }
    for c in contributions {
        if !c.rls_visible {
            return Err(AiRetrievalError::RlsHygiene {
                tablet_id: c.tablet_id,
                row_id: c.row_id.0,
            });
        }
        if c.local_rank == 0 {
            return Err(AiRetrievalError::Protocol(
                "local_rank must be 1-based (got 0)".to_owned(),
            ));
        }
        if !c.raw_score.is_finite() || !c.weight.is_finite() {
            return Err(AiRetrievalError::Protocol(
                "contribution scores and weights must be finite".to_owned(),
            ));
        }
    }

    // Dedup: one contribution per (row, retriever).
    let mut best: HashMap<(RowId, String), LocalRetrieverContribution> = HashMap::new();
    for c in contributions {
        let key = (c.row_id, c.retriever_id.clone());
        best.entry(key)
            .and_modify(|existing| {
                if contribution_better(c, existing) {
                    *existing = c.clone();
                }
            })
            .or_insert_with(|| c.clone());
    }

    // Group by row.
    let mut by_row: HashMap<RowId, Vec<LocalRetrieverContribution>> = HashMap::new();
    for c in best.into_values() {
        by_row.entry(c.row_id).or_default().push(c);
    }

    let mut fused = Vec::with_capacity(by_row.len());
    for (row_id, mut comps) in by_row {
        comps.sort_by(|a, b| {
            a.retriever_id
                .cmp(&b.retriever_id)
                .then_with(|| a.tablet_id.cmp(&b.tablet_id))
        });
        let tablet_id = comps
            .iter()
            .map(|c| c.tablet_id)
            .min()
            .expect("row group is non-empty");
        let raw_score = comps
            .iter()
            .map(|c| c.raw_score)
            .fold(f64::NEG_INFINITY, f64::max);
        let (final_score, components) = match method {
            FusionMethod::Rrf { k } => {
                let mut components = Vec::with_capacity(comps.len());
                let mut score = 0.0;
                for c in &comps {
                    let contribution = c.weight / (f64::from(k) + f64::from(c.local_rank));
                    if !contribution.is_finite() {
                        return Err(AiRetrievalError::Protocol(
                            "RRF contribution must be finite".to_owned(),
                        ));
                    }
                    score += contribution;
                    components.push(HybridComponentScore {
                        retriever_id: c.retriever_id.clone(),
                        retriever_kind: c.retriever_kind.clone(),
                        tablet_id: c.tablet_id,
                        rank: c.local_rank,
                        raw_score: c.raw_score,
                        contribution,
                        weight: c.weight,
                    });
                }
                (score, components)
            }
            FusionMethod::MaxScore => {
                let mut components = Vec::with_capacity(comps.len());
                let mut score = f64::NEG_INFINITY;
                for c in &comps {
                    let contribution = c.raw_score * c.weight;
                    score = score.max(contribution);
                    components.push(HybridComponentScore {
                        retriever_id: c.retriever_id.clone(),
                        retriever_kind: c.retriever_kind.clone(),
                        tablet_id: c.tablet_id,
                        rank: c.local_rank,
                        raw_score: c.raw_score,
                        contribution,
                        weight: c.weight,
                    });
                }
                (score, components)
            }
        };
        if !final_score.is_finite() {
            return Err(AiRetrievalError::Protocol(
                "fused score must be finite".to_owned(),
            ));
        }
        fused.push(HybridMergedCandidate {
            tablet_id,
            row_id,
            final_score,
            raw_score,
            components,
        });
    }

    fused.sort_by(hybrid_candidate_cmp);
    if fused.len() > budget.candidate_ceiling {
        fused.truncate(budget.candidate_ceiling);
    }

    let refill = hybrid_refill_targets(&fused, budget.candidate_ceiling, method, bounds);
    Ok(HybridMergeResult {
        candidates: fused,
        refill,
    })
}

/// True when `a` is a better contribution than `b` for the same
/// `(row, retriever)` key.
fn contribution_better(a: &LocalRetrieverContribution, b: &LocalRetrieverContribution) -> bool {
    a.local_rank < b.local_rank
        || (a.local_rank == b.local_rank
            && (a.raw_score > b.raw_score
                || (a.raw_score == b.raw_score && a.tablet_id < b.tablet_id)))
}

/// Ranking order: fused score desc, tablet id asc, row id asc.
fn hybrid_candidate_cmp(a: &HybridMergedCandidate, b: &HybridMergedCandidate) -> Ordering {
    b.final_score
        .partial_cmp(&a.final_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| a.tablet_id.cmp(&b.tablet_id))
        .then_with(|| a.row_id.cmp(&b.row_id))
}

/// Adaptive refill API: which `(tablet, retriever)` streams could still
/// contribute a global top-`k` winner given the current merge result and the
/// per-stream unseen bounds.
///
/// A stream is refilled when it is not exhausted and either:
/// * fewer than `k` winners are known, or
/// * the optimistic single-retriever contribution of its next unseen hit
///   (`weight / (k_rrf + next_rank)` for RRF, or `weight * unseen_score_bound`
///   for max-score) is not strictly worse than the current `k`-th winner.
pub fn hybrid_refill_targets(
    winners: &[HybridMergedCandidate],
    k: usize,
    method: FusionMethod,
    bounds: &[RetrieverTabletBound],
) -> Vec<(TabletId, String)> {
    if k == 0 || bounds.is_empty() {
        return Vec::new();
    }
    let mut refill = Vec::new();
    let threshold = if winners.len() < k {
        None
    } else {
        Some(winners[k - 1].final_score)
    };
    for bound in bounds {
        if bound.exhausted {
            continue;
        }
        let Some(next_rank) = bound.next_rank.filter(|r| *r > 0) else {
            continue;
        };
        let optimistic = match method {
            FusionMethod::Rrf { k: rrf_k } => {
                bound.weight / (f64::from(rrf_k) + f64::from(next_rank))
            }
            FusionMethod::MaxScore => bound
                .unseen_score_bound
                .map(|s| s * bound.weight)
                .unwrap_or(f64::INFINITY),
        };
        if !optimistic.is_finite() {
            refill.push((bound.tablet_id, bound.retriever_id.clone()));
            continue;
        }
        match threshold {
            None => refill.push((bound.tablet_id, bound.retriever_id.clone())),
            Some(t) if optimistic >= t => {
                refill.push((bound.tablet_id, bound.retriever_id.clone()));
            }
            Some(_) => {}
        }
    }
    refill.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    refill.dedup();
    refill
}

/// Drives [`merge_hybrid_contributions`] with adaptive refill until the result
/// is exact under the provided bounds (or the work budget is exhausted).
///
/// `refill_batch(tablet, retriever)` must return the next batch of local
/// contributions for that stream plus an updated bound. Returning an empty
/// batch with an unchanged non-exhausted bound is a protocol error.
pub fn exact_hybrid_merge<F>(
    mut contributions: Vec<LocalRetrieverContribution>,
    mut bounds: HashMap<(TabletId, String), RetrieverTabletBound>,
    method: FusionMethod,
    budget: &AiWorkBudget,
    global_k: usize,
    mut refill_batch: F,
) -> Result<Vec<HybridMergedCandidate>, AiRetrievalError>
where
    F: FnMut(
        TabletId,
        &str,
    )
        -> Result<(Vec<LocalRetrieverContribution>, RetrieverTabletBound), AiRetrievalError>,
{
    let mut rounds = 0usize;
    let max_refill_rounds = budget.max_refill_rounds.max(1);
    loop {
        if contributions.len() > budget.max_local_candidates {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "hybrid contributions {} exceed max {}",
                contributions.len(),
                budget.max_local_candidates
            )));
        }
        if contributions.len() > budget.max_retained_contributions {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "hybrid contributions {} exceed max_retained_contributions {}",
                contributions.len(),
                budget.max_retained_contributions
            )));
        }
        let bound_list: Vec<RetrieverTabletBound> = bounds.values().cloned().collect();
        let mut step_budget = budget.clone();
        // Intermediate merge keeps all fused rows so refill decisions see the
        // true k-th threshold; the final return is truncated to global_k.
        step_budget.candidate_ceiling = step_budget.candidate_ceiling.max(global_k).max(1);
        let mut result =
            merge_hybrid_contributions(&contributions, method, &step_budget, &bound_list)?;
        if global_k > 0 && result.candidates.len() > global_k {
            result.candidates.truncate(global_k);
            // Recompute refill against the truncated winner list.
            result.refill =
                hybrid_refill_targets(&result.candidates, global_k, method, &bound_list);
        }
        if result.refill.is_empty() {
            if result.candidates.len() > budget.candidate_ceiling {
                result.candidates.truncate(budget.candidate_ceiling);
            }
            return Ok(result.candidates);
        }
        rounds += 1;
        if rounds > max_refill_rounds {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "hybrid adaptive refill exceeded {max_refill_rounds} rounds"
            )));
        }
        for (tablet, retriever) in result.refill {
            let (batch, new_bound) = refill_batch(tablet, &retriever)?;
            let key = (tablet, retriever.clone());
            let previous = bounds.get(&key).cloned();
            if batch.is_empty()
                && previous
                    .as_ref()
                    .is_some_and(|p| !p.exhausted && p.next_rank == new_bound.next_rank)
            {
                return Err(AiRetrievalError::Protocol(format!(
                    "hybrid refill for tablet {tablet} retriever `{retriever}` made no progress"
                )));
            }
            for c in &batch {
                if c.tablet_id != tablet || c.retriever_id != retriever {
                    return Err(AiRetrievalError::Protocol(format!(
                        "hybrid refill batch must match tablet {tablet} retriever `{retriever}`"
                    )));
                }
            }
            contributions.extend(batch);
            bounds.insert(key, new_bound);
        }
    }
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

// ---------------------------------------------------------------------------
// Authenticated remote tablet retrieval
// ---------------------------------------------------------------------------

/// Stable service discriminator inside the cluster internal-RPC multiplexer.
pub const REMOTE_AI_SERVICE_ID: u32 = 2;
/// Current private distributed-AI wire generation.
pub const REMOTE_AI_PROTOCOL_VERSION: u16 = 1;
/// Default maximum request or response body.
pub const DEFAULT_REMOTE_AI_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
/// Default maximum concurrent tablet retrievals held by one worker.
pub const DEFAULT_REMOTE_AI_EXECUTIONS: usize = 256;
/// Maximum forwarded authorization-context bytes.
pub const MAX_AI_AUTHORIZATION_CONTEXT_BYTES: usize = 64 * 1024;

/// One typed tablet-local search request.
///
/// `authorization_context` is an opaque, bounded server-issued identity
/// envelope. The worker executor must validate it and apply column grants,
/// RLS, and masks before returning candidates. The transport never treats
/// node mTLS alone as user authorization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTabletQuery {
    /// Global query identity.
    pub query_id: QueryId,
    /// Tablet whose replica must execute this request.
    pub tablet_id: TabletId,
    /// Table name inside that tablet.
    pub table: String,
    /// Core hybrid-search request, bounded to tablet-local `k` by the
    /// coordinator.
    pub search: SearchRequest,
    /// Server-issued authorization envelope.
    pub authorization_context: Vec<u8>,
    /// Remaining deadline propagated to the worker.
    pub deadline_ms: Option<u64>,
    /// Global work budget.
    pub budget: AiWorkBudget,
}

/// Tablet response metadata for distributed AI (P0.8-T2).
///
/// Carries retriever exhaustion / unseen bounds, index and read HLCs, and the
/// model generation that produced the hit. The coordinator fail-closes when
/// model generations disagree across tablets or with
/// [`AiWorkBudget::expected_model_generation`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TabletAiResponseMetadata {
    /// True when every named local retriever stream on this tablet is exhausted.
    #[serde(default)]
    pub retriever_exhausted: bool,
    /// Upper bound on the score of any unseen local hit (`None` if exhausted
    /// or unknown). Mirrors the tightest `upper_bound_after` across streams.
    #[serde(default)]
    pub unseen_score_bound: Option<f64>,
    /// Index `applied_through` HLC on the serving replica (`physical.logical.node`).
    #[serde(default)]
    pub index_applied_hlc: Option<String>,
    /// Read HLC used for this tablet-local search.
    #[serde(default)]
    pub read_hlc: Option<String>,
    /// Embedding / ANN model generation id for this tablet's index.
    #[serde(default)]
    pub model_generation: Option<u64>,
}

/// One tablet result after authorization and optional exact rerank.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiTabletHit {
    /// Ranked candidate used by the deterministic global merge.
    pub candidate: LocalCandidate,
    /// Projected, already-masked cells.
    pub cells: Vec<(u16, Value)>,
    /// Exact-vector rerank score when requested.
    pub exact_rerank_score: Option<f32>,
    /// Per-replica consistency evidence.
    pub consistency: Option<AiConsistencyAudit>,
    /// Per-retriever contributions for global hybrid fusion (P0.8).
    ///
    /// When non-empty (or the request names multiple retrievers), the
    /// coordinator uses [`merge_hybrid_contributions`] instead of
    /// single-score [`merge_candidates`]. Empty means a single local-rank
    /// contribution derived from [`Self::candidate`].
    #[serde(default)]
    pub contributions: Vec<LocalRetrieverContribution>,
    /// Tablet response metadata (exhaustion, HLC, model generation) — P0.8-T2.
    #[serde(default)]
    pub metadata: TabletAiResponseMetadata,
}

/// Worker implementation for tablet-local AI search.
#[async_trait::async_trait]
pub trait AiTabletExecutor: Send + Sync {
    /// Executes one request. Implementations must validate
    /// `authorization_context` and enforce RLS before local top-k.
    async fn retrieve(
        &self,
        request: &AiTabletQuery,
        control: ExecutionControl,
    ) -> Result<Vec<AiTabletHit>, AiRetrievalError>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteAiEnvelope {
    version: u16,
    request: RemoteAiRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoteAiRequest {
    Retrieve(Box<AiTabletQuery>),
    Cancel {
        query_id: QueryId,
        tablet_id: TabletId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RemoteAiResponseEnvelope {
    version: u16,
    response: RemoteAiResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum RemoteAiResponse {
    Hits(Vec<AiTabletHit>),
    Cancelled,
    Error(String),
}

type AiExecutionKey = (QueryId, TabletId);

/// Worker-side endpoint for the distributed-AI protocol.
pub struct RemoteAiEndpoint {
    executor: Arc<dyn AiTabletExecutor>,
    controls: parking_lot::Mutex<HashMap<AiExecutionKey, ExecutionControl>>,
    max_executions: usize,
    max_message_bytes: usize,
}

impl RemoteAiEndpoint {
    /// Creates a bounded endpoint.
    pub fn new(executor: Arc<dyn AiTabletExecutor>) -> Self {
        Self::with_limits(
            executor,
            DEFAULT_REMOTE_AI_EXECUTIONS,
            DEFAULT_REMOTE_AI_MESSAGE_BYTES,
        )
    }

    /// Creates an endpoint with explicit concurrency and message bounds.
    pub fn with_limits(
        executor: Arc<dyn AiTabletExecutor>,
        max_executions: usize,
        max_message_bytes: usize,
    ) -> Self {
        Self {
            executor,
            controls: parking_lot::Mutex::new(HashMap::new()),
            max_executions: max_executions.max(1),
            max_message_bytes: max_message_bytes.max(1),
        }
    }

    /// Number of running tablet requests.
    pub fn active_executions(&self) -> usize {
        self.controls.lock().len()
    }

    /// Handles one authenticated internal RPC body.
    pub async fn handle(&self, bytes: &[u8]) -> Result<Vec<u8>, AiRetrievalError> {
        let envelope: RemoteAiEnvelope = decode_ai_wire(bytes, self.max_message_bytes)?;
        if envelope.version != REMOTE_AI_PROTOCOL_VERSION {
            return Err(AiRetrievalError::Protocol(format!(
                "unsupported distributed-AI protocol version {}; supported version is {}",
                envelope.version, REMOTE_AI_PROTOCOL_VERSION
            )));
        }
        let response = match self.handle_request(envelope.request).await {
            Ok(response) => response,
            Err(error) => RemoteAiResponse::Error(error.to_string()),
        };
        encode_ai_wire(
            &RemoteAiResponseEnvelope {
                version: REMOTE_AI_PROTOCOL_VERSION,
                response,
            },
            self.max_message_bytes,
        )
    }

    async fn handle_request(
        &self,
        request: RemoteAiRequest,
    ) -> Result<RemoteAiResponse, AiRetrievalError> {
        match request {
            RemoteAiRequest::Retrieve(request) => {
                if request.authorization_context.len() > MAX_AI_AUTHORIZATION_CONTEXT_BYTES {
                    return Err(AiRetrievalError::Protocol(format!(
                        "authorization context is {} bytes; limit is {}",
                        request.authorization_context.len(),
                        MAX_AI_AUTHORIZATION_CONTEXT_BYTES
                    )));
                }
                let key = (request.query_id, request.tablet_id);
                let control = request.deadline_ms.map_or_else(
                    || ExecutionControl::new(None),
                    |milliseconds| {
                        ExecutionControl::with_timeout(Duration::from_millis(milliseconds))
                    },
                );
                {
                    let mut controls = self.controls.lock();
                    if controls.contains_key(&key) {
                        return Err(AiRetrievalError::Protocol(format!(
                            "query {} is already retrieving tablet {}",
                            request.query_id, request.tablet_id
                        )));
                    }
                    if controls.len() >= self.max_executions {
                        return Err(AiRetrievalError::BudgetExceeded(format!(
                            "worker holds {} AI requests; limit is {}",
                            controls.len(),
                            self.max_executions
                        )));
                    }
                    controls.insert(key, control.clone());
                }
                let result = self.executor.retrieve(&request, control.clone()).await;
                let was_active = self.controls.lock().remove(&key).is_some();
                if !was_active || control.checkpoint().is_err() {
                    return Err(AiRetrievalError::Cancelled(control.reason()));
                }
                let hits = result?;
                for hit in &hits {
                    if hit.candidate.tablet_id != request.tablet_id {
                        return Err(AiRetrievalError::Protocol(format!(
                            "tablet {} returned candidate labeled {}",
                            request.tablet_id, hit.candidate.tablet_id
                        )));
                    }
                    if !hit.candidate.rls_visible {
                        return Err(AiRetrievalError::RlsHygiene {
                            tablet_id: hit.candidate.tablet_id,
                            row_id: hit.candidate.row_id.0,
                        });
                    }
                }
                Ok(RemoteAiResponse::Hits(hits))
            }
            RemoteAiRequest::Cancel {
                query_id,
                tablet_id,
            } => {
                if let Some(control) = self.controls.lock().get(&(query_id, tablet_id)).cloned() {
                    control.cancel(CancellationReason::ClientRequest);
                }
                Ok(RemoteAiResponse::Cancelled)
            }
        }
    }
}

/// One authenticated request/response carrier for distributed AI.
#[async_trait::async_trait]
pub trait AiRpcClient: Send + Sync {
    /// Performs one bounded internal RPC.
    async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, AiRetrievalError>;
}

/// In-process carrier for endpoint tests.
pub struct LoopbackAiRpcClient {
    endpoint: Arc<RemoteAiEndpoint>,
}

impl LoopbackAiRpcClient {
    /// Wraps one worker endpoint.
    pub fn new(endpoint: Arc<RemoteAiEndpoint>) -> Self {
        Self { endpoint }
    }
}

#[async_trait::async_trait]
impl AiRpcClient for LoopbackAiRpcClient {
    async fn call(&self, request: Vec<u8>) -> Result<Vec<u8>, AiRetrievalError> {
        self.endpoint.handle(&request).await
    }
}

/// Fully merged result plus authorized tablet payloads for the winners.
#[derive(Debug, Clone)]
pub struct DistributedAiResult {
    /// Deterministic global winners.
    pub candidates: Vec<MergedCandidate>,
    /// Winner payloads in the same order as `candidates`.
    pub hits: Vec<AiTabletHit>,
}

/// Production coordinator fusion for distributed AI / Kit search (P0.8).
///
/// When the request names multiple retrievers **or** any tablet hit carries
/// per-retriever [`LocalRetrieverContribution`]s, this uses
/// [`merge_hybrid_contributions`] so dense+sparse(+MinHash) RRF is global.
/// Otherwise it falls back to single-score [`merge_candidates`].
///
/// This is the function the product path must call — unit tests of
/// `merge_hybrid_contributions` alone do not exercise production wiring.
pub fn fuse_distributed_hits(
    hits: &[AiTabletHit],
    search: &SearchRequest,
    method: FusionMethod,
    budget: &AiWorkBudget,
) -> Result<Vec<MergedCandidate>, AiRetrievalError> {
    validate_hit_metadata(hits, budget)?;
    let multi_retriever = search.retrievers.len() > 1;
    let has_contributions = hits.iter().any(|hit| !hit.contributions.is_empty());
    let mut candidates = if multi_retriever || has_contributions {
        let contributions = collect_hit_contributions(hits, search)?;
        if contributions.len() > budget.max_retained_contributions {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "{} contributions > max_retained_contributions {}",
                contributions.len(),
                budget.max_retained_contributions
            )));
        }
        let hybrid = merge_hybrid_contributions(&contributions, method, budget, &[])?;
        hybrid
            .candidates
            .into_iter()
            .map(|c| MergedCandidate {
                tablet_id: c.tablet_id,
                row_id: c.row_id,
                final_score: c.final_score,
                raw_score: c.raw_score,
            })
            .collect()
    } else {
        let locals = hits
            .iter()
            .map(|hit| hit.candidate.clone())
            .collect::<Vec<_>>();
        merge_candidates(&locals, method, budget)?
    };
    apply_exact_global_rerank(&mut candidates, hits, search, budget)?;
    Ok(candidates)
}

/// Exact global rerank (P0.8-T3 / X7): blend fused score with tablet-supplied
/// exact-vector scores and re-order winners.
///
/// When the search requests [`Rerank::ExactVector`] or hits already carry
/// `exact_rerank_score`, final score becomes `fused + weight * exact` (weight
/// defaults to 1.0 when only payload scores are present). Candidates without
/// an exact score keep their fused score. Ordering is final_score desc,
/// tablet id asc, row id asc — matching the local search path.
fn apply_exact_global_rerank(
    candidates: &mut Vec<MergedCandidate>,
    hits: &[AiTabletHit],
    search: &SearchRequest,
    budget: &AiWorkBudget,
) -> Result<(), AiRetrievalError> {
    let has_scores = hits.iter().any(|hit| hit.exact_rerank_score.is_some());
    if !has_scores {
        return Ok(());
    }
    let (weight, candidate_limit) = match &search.rerank {
        Some(Rerank::ExactVector {
            candidate_limit,
            weight,
            ..
        }) => {
            if !weight.is_finite() || *weight < 0.0 {
                return Err(AiRetrievalError::Protocol(
                    "exact rerank weight must be finite and non-negative".to_owned(),
                ));
            }
            (*weight, (*candidate_limit).max(1))
        }
        // Tablet workers already applied exact scoring; re-order globally.
        _ => (1.0, candidates.len().max(1)),
    };
    if candidates.len() > candidate_limit {
        candidates.truncate(candidate_limit);
    }
    if candidates.len() > budget.max_exact_gathers {
        return Err(AiRetrievalError::BudgetExceeded(format!(
            "{} exact gather candidates > max_exact_gathers {}",
            candidates.len(),
            budget.max_exact_gathers
        )));
    }
    // Prefer exact key match; fall back to row_id when hybrid merge picks a
    // different tablet_id among multi-tablet contributions for the same row.
    let mut by_key: HashMap<(TabletId, RowId), f32> = HashMap::new();
    let mut by_row: HashMap<RowId, f32> = HashMap::new();
    for hit in hits {
        let Some(score) = hit.exact_rerank_score else {
            continue;
        };
        if !score.is_finite() {
            return Err(AiRetrievalError::Protocol(
                "exact_rerank_score must be finite".to_owned(),
            ));
        }
        by_key.insert((hit.candidate.tablet_id, hit.candidate.row_id), score);
        by_row
            .entry(hit.candidate.row_id)
            .and_modify(|existing| {
                if score > *existing {
                    *existing = score;
                }
            })
            .or_insert(score);
    }
    for candidate in candidates.iter_mut() {
        let exact = by_key
            .get(&(candidate.tablet_id, candidate.row_id))
            .or_else(|| by_row.get(&candidate.row_id));
        if let Some(score) = exact {
            let blended = candidate.final_score + weight * f64::from(*score);
            if !blended.is_finite() {
                return Err(AiRetrievalError::Protocol(
                    "exact-reranked final_score must be finite".to_owned(),
                ));
            }
            candidate.final_score = blended;
        }
    }
    candidates.sort_by(|a, b| {
        b.final_score
            .partial_cmp(&a.final_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.tablet_id.cmp(&b.tablet_id))
            .then_with(|| a.row_id.cmp(&b.row_id))
    });
    if candidates.len() > budget.candidate_ceiling {
        candidates.truncate(budget.candidate_ceiling);
    }
    Ok(())
}

/// Fail-closed validation of tablet metadata (model generation, payload cells).
fn validate_hit_metadata(
    hits: &[AiTabletHit],
    budget: &AiWorkBudget,
) -> Result<(), AiRetrievalError> {
    let mut total_cells = 0usize;
    let mut observed_generation: Option<u64> = None;
    for hit in hits {
        total_cells = total_cells.saturating_add(hit.cells.len());
        if let Some(gen) = hit.metadata.model_generation {
            if let Some(expected) = budget.expected_model_generation {
                if gen != expected {
                    return Err(AiRetrievalError::Protocol(format!(
                        "tablet model generation {gen} != expected {expected}"
                    )));
                }
            }
            match observed_generation {
                None => observed_generation = Some(gen),
                Some(prev) if prev != gen => {
                    return Err(AiRetrievalError::Protocol(format!(
                        "model generation mismatch across tablets: {prev} vs {gen}"
                    )));
                }
                Some(_) => {}
            }
        } else if let Some(expected) = budget.expected_model_generation {
            // Coordinator requested a generation but tablet omitted it.
            return Err(AiRetrievalError::Protocol(format!(
                "tablet omitted model_generation; expected {expected}"
            )));
        }
    }
    if total_cells > budget.max_payload_cells {
        return Err(AiRetrievalError::BudgetExceeded(format!(
            "{total_cells} payload cells > max {}",
            budget.max_payload_cells
        )));
    }
    Ok(())
}

/// Flattens tablet hits into per-retriever contributions for global hybrid merge.
fn collect_hit_contributions(
    hits: &[AiTabletHit],
    search: &SearchRequest,
) -> Result<Vec<LocalRetrieverContribution>, AiRetrievalError> {
    let weight_by_name: HashMap<&str, f64> = search
        .retrievers
        .iter()
        .map(|named| (named.name.as_str(), named.weight))
        .collect();
    let mut out = Vec::new();
    for hit in hits {
        if hit.contributions.is_empty() {
            // Tablet already fused or single-retriever payload: one synthetic
            // contribution so hybrid merge still runs for multi-retriever
            // requests (prefer real components when workers provide them).
            let retriever_id = search
                .retrievers
                .first()
                .map(|named| named.name.clone())
                .unwrap_or_else(|| "local".to_owned());
            let weight = weight_by_name
                .get(retriever_id.as_str())
                .copied()
                .unwrap_or(1.0);
            out.push(LocalRetrieverContribution {
                tablet_id: hit.candidate.tablet_id,
                row_id: hit.candidate.row_id,
                retriever_id,
                retriever_kind: None,
                local_rank: hit.candidate.local_rank.max(1),
                raw_score: hit.candidate.score,
                upper_bound_after: None,
                rls_visible: hit.candidate.rls_visible,
                weight,
            });
            continue;
        }
        for mut contribution in hit.contributions.iter().cloned() {
            if contribution.tablet_id != hit.candidate.tablet_id {
                contribution.tablet_id = hit.candidate.tablet_id;
            }
            if contribution.row_id != hit.candidate.row_id {
                return Err(AiRetrievalError::Protocol(format!(
                    "contribution row {} does not match hit row {}",
                    contribution.row_id.0, hit.candidate.row_id.0
                )));
            }
            if let Some(weight) = weight_by_name.get(contribution.retriever_id.as_str()) {
                contribution.weight = *weight;
            }
            out.push(contribution);
        }
    }
    Ok(out)
}

/// Borrowed inputs for one coordinator-side distributed-AI fan-out.
pub struct AiFanoutRequest<'a> {
    /// Stable query identity propagated to every tablet.
    pub query_id: QueryId,
    /// Tablets participating in the fan-out.
    pub tablets: &'a [TabletId],
    /// Authoritative table name.
    pub table: &'a str,
    /// Scored-search request.
    pub search: &'a SearchRequest,
    /// Server-issued authorization envelope.
    pub authorization_context: &'a [u8],
    /// Deterministic global fusion policy.
    pub fusion: FusionMethod,
    /// Tablet-local candidate overfetch multiplier.
    pub overfetch_factor: f64,
    /// Global deadline, work, candidate, and memory limits.
    pub budget: &'a AiWorkBudget,
    /// Parent cancellation and deadline control.
    pub control: &'a ExecutionControl,
}

/// Coordinator-side distributed-AI fan-out.
pub struct RemoteAiTransport {
    default_client: Option<Arc<dyn AiRpcClient>>,
    clients: parking_lot::RwLock<HashMap<TabletId, Arc<dyn AiRpcClient>>>,
    max_message_bytes: usize,
}

impl RemoteAiTransport {
    /// Creates a transport whose tablets use `default_client`.
    pub fn new(default_client: Arc<dyn AiRpcClient>) -> Self {
        Self {
            default_client: Some(default_client),
            clients: parking_lot::RwLock::new(HashMap::new()),
            max_message_bytes: DEFAULT_REMOTE_AI_MESSAGE_BYTES,
        }
    }

    /// Creates a fail-closed transport with no fallback tablet route.
    pub fn routed() -> Self {
        Self {
            default_client: None,
            clients: parking_lot::RwLock::new(HashMap::new()),
            max_message_bytes: DEFAULT_REMOTE_AI_MESSAGE_BYTES,
        }
    }

    /// Routes one tablet to a specific peer.
    pub fn with_client(self, tablet: TabletId, client: Arc<dyn AiRpcClient>) -> Self {
        self.clients.write().insert(tablet, client);
        self
    }

    fn client_for(&self, tablet: TabletId) -> Result<Arc<dyn AiRpcClient>, AiRetrievalError> {
        self.clients
            .read()
            .get(&tablet)
            .cloned()
            .or_else(|| self.default_client.as_ref().map(Arc::clone))
            .ok_or_else(|| {
                AiRetrievalError::Transport(format!(
                    "no authenticated AI route for tablet {tablet}"
                ))
            })
    }

    /// Fans out a typed search, enforces the global budget, and merges the
    /// authorized tablet-local top-k results.
    pub async fn retrieve(
        &self,
        request: AiFanoutRequest<'_>,
    ) -> Result<DistributedAiResult, AiRetrievalError> {
        let AiFanoutRequest {
            query_id,
            tablets,
            table,
            search,
            authorization_context,
            fusion,
            overfetch_factor,
            budget,
            control,
        } = request;
        if tablets.is_empty() {
            return Ok(DistributedAiResult {
                candidates: Vec::new(),
                hits: Vec::new(),
            });
        }
        if tablets.len() > budget.max_tablet_rpcs {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "{} tablets > max_tablet_rpcs {}",
                tablets.len(),
                budget.max_tablet_rpcs
            )));
        }
        if authorization_context.len() > MAX_AI_AUTHORIZATION_CONTEXT_BYTES {
            return Err(AiRetrievalError::Protocol(format!(
                "authorization context is {} bytes; limit is {}",
                authorization_context.len(),
                MAX_AI_AUTHORIZATION_CONTEXT_BYTES
            )));
        }
        if !overfetch_factor.is_finite() || overfetch_factor <= 0.0 {
            return Err(AiRetrievalError::Protocol(
                "overfetch factor must be finite and positive".to_owned(),
            ));
        }
        let request_control = control.child_with_timeout(Duration::from_millis(budget.deadline_ms));
        request_control
            .checkpoint()
            .map_err(|_| AiRetrievalError::Cancelled(request_control.reason()))?;
        let local_k = adaptive_local_k(budget.candidate_ceiling, overfetch_factor, tablets.len());
        let total_requested = local_k
            .checked_mul(tablets.len())
            .ok_or_else(|| AiRetrievalError::BudgetExceeded("candidate count overflow".into()))?;
        if total_requested > budget.max_local_candidates {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "{total_requested} requested local candidates > max {}",
                budget.max_local_candidates
            )));
        }
        let mut local_search = search.clone();
        local_search.limit = local_k;
        for named in &mut local_search.retrievers {
            match &mut named.retriever {
                Retriever::Ann { k, .. }
                | Retriever::Sparse { k, .. }
                | Retriever::MinHash { k, .. } => *k = local_k,
            }
        }
        if let Some(Rerank::ExactVector {
            candidate_limit, ..
        }) = &mut local_search.rerank
        {
            *candidate_limit = (*candidate_limit).max(local_k);
        }

        let mut calls = FuturesUnordered::new();
        for &tablet_id in tablets {
            let client = self.client_for(tablet_id)?;
            let request = AiTabletQuery {
                query_id,
                tablet_id,
                table: table.to_owned(),
                search: local_search.clone(),
                authorization_context: authorization_context.to_vec(),
                deadline_ms: request_control
                    .remaining_duration()
                    .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64),
                budget: budget.clone(),
            };
            let max_message_bytes = self.max_message_bytes;
            calls.push(async move {
                let response = ai_remote_call(
                    &client,
                    RemoteAiRequest::Retrieve(Box::new(request)),
                    max_message_bytes,
                )
                .await;
                (tablet_id, client, response)
            });
        }
        let mut hits = Vec::new();
        let mut retained_bytes = 0u64;
        while !calls.is_empty() {
            tokio::select! {
                _ = request_control.cancelled() => {
                    for &tablet_id in tablets {
                        let client = self.client_for(tablet_id)?;
                        let max_message_bytes = self.max_message_bytes;
                        tokio::spawn(async move {
                            let _ = ai_remote_call(
                                &client,
                                RemoteAiRequest::Cancel { query_id, tablet_id },
                                max_message_bytes,
                            ).await;
                        });
                    }
                    return Err(AiRetrievalError::Cancelled(request_control.reason()));
                }
                result = calls.next() => {
                    let Some((tablet_id, _client, response)) = result else {
                        break;
                    };
                    let response = match response {
                        Ok(response) => response,
                        Err(error) => {
                            self.cancel_tablets(query_id, tablets);
                            return Err(error);
                        }
                    };
                    match response {
                        RemoteAiResponse::Hits(mut tablet_hits) => {
                            if tablet_hits.len() > local_k {
                                self.cancel_tablets(query_id, tablets);
                                return Err(AiRetrievalError::Protocol(format!(
                                    "tablet {tablet_id} returned {} candidates; local limit is {local_k}",
                                    tablet_hits.len()
                                )));
                            }
                            let bytes = ai_wire_options()
                                .serialized_size(&tablet_hits)
                                .map_err(|error| AiRetrievalError::Protocol(error.to_string()))?;
                            retained_bytes = retained_bytes.saturating_add(bytes);
                            if retained_bytes > budget.memory_bytes {
                                self.cancel_tablets(query_id, tablets);
                                return Err(AiRetrievalError::BudgetExceeded(format!(
                                    "{retained_bytes} retained result bytes > memory budget {}",
                                    budget.memory_bytes
                                )));
                            }
                            hits.append(&mut tablet_hits);
                        }
                        other => {
                            self.cancel_tablets(query_id, tablets);
                            return Err(AiRetrievalError::Protocol(format!(
                                "expected AI Hits response, got {other:?}"
                            )));
                        }
                    }
                }
            }
        }
        if hits.len() > budget.max_local_candidates {
            return Err(AiRetrievalError::BudgetExceeded(format!(
                "{} returned local candidates > max {}",
                hits.len(),
                budget.max_local_candidates
            )));
        }
        // P0.8 production path: multi-retriever / contribution-bearing hits
        // fuse via merge_hybrid_contributions (not single local_rank RRF).
        let candidates = fuse_distributed_hits(&hits, search, fusion, budget)?;
        let by_key = hits
            .into_iter()
            .map(|hit| ((hit.candidate.tablet_id, hit.candidate.row_id), hit))
            .collect::<HashMap<_, _>>();
        let winner_hits = candidates
            .iter()
            .map(|candidate| {
                by_key
                    .get(&(candidate.tablet_id, candidate.row_id))
                    .cloned()
                    .or_else(|| {
                        // Hybrid merge may pick the lowest tablet id among
                        // contributions; match by row_id when the exact
                        // (tablet, row) key is missing.
                        by_key
                            .iter()
                            .find(|((_, row), _)| *row == candidate.row_id)
                            .map(|(_, hit)| hit.clone())
                    })
                    .ok_or_else(|| {
                        AiRetrievalError::Protocol(format!(
                            "winner {} from tablet {} has no payload",
                            candidate.row_id, candidate.tablet_id
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DistributedAiResult {
            candidates,
            hits: winner_hits,
        })
    }

    fn cancel_tablets(&self, query_id: QueryId, tablets: &[TabletId]) {
        for &tablet_id in tablets {
            let Ok(client) = self.client_for(tablet_id) else {
                continue;
            };
            let max_message_bytes = self.max_message_bytes;
            tokio::spawn(async move {
                let _ = ai_remote_call(
                    &client,
                    RemoteAiRequest::Cancel {
                        query_id,
                        tablet_id,
                    },
                    max_message_bytes,
                )
                .await;
            });
        }
    }
}

async fn ai_remote_call(
    client: &Arc<dyn AiRpcClient>,
    request: RemoteAiRequest,
    max_message_bytes: usize,
) -> Result<RemoteAiResponse, AiRetrievalError> {
    let bytes = encode_ai_wire(
        &RemoteAiEnvelope {
            version: REMOTE_AI_PROTOCOL_VERSION,
            request,
        },
        max_message_bytes,
    )?;
    let bytes = client.call(bytes).await?;
    let response: RemoteAiResponseEnvelope = decode_ai_wire(&bytes, max_message_bytes)?;
    if response.version != REMOTE_AI_PROTOCOL_VERSION {
        return Err(AiRetrievalError::Protocol(format!(
            "peer answered with distributed-AI protocol version {}; supported version is {}",
            response.version, REMOTE_AI_PROTOCOL_VERSION
        )));
    }
    match response.response {
        RemoteAiResponse::Error(message) => Err(AiRetrievalError::Transport(message)),
        response => Ok(response),
    }
}

fn ai_wire_options() -> impl Options {
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
}

fn encode_ai_wire<T: Serialize>(
    value: &T,
    max_message_bytes: usize,
) -> Result<Vec<u8>, AiRetrievalError> {
    let bytes = ai_wire_options()
        .serialize(value)
        .map_err(|error| AiRetrievalError::Protocol(error.to_string()))?;
    if bytes.len() > max_message_bytes {
        return Err(AiRetrievalError::Protocol(format!(
            "encoded distributed-AI message is {} bytes; limit is {max_message_bytes}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

fn decode_ai_wire<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    max_message_bytes: usize,
) -> Result<T, AiRetrievalError> {
    if bytes.len() > max_message_bytes {
        return Err(AiRetrievalError::Protocol(format!(
            "distributed-AI message is {} bytes; limit is {max_message_bytes}",
            bytes.len()
        )));
    }
    ai_wire_options()
        .with_limit(max_message_bytes as u64)
        .deserialize(bytes)
        .map_err(|error| AiRetrievalError::Protocol(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use mongreldb_core::query::Fusion;

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

    struct StaticExecutor;

    #[async_trait::async_trait]
    impl AiTabletExecutor for StaticExecutor {
        async fn retrieve(
            &self,
            request: &AiTabletQuery,
            control: ExecutionControl,
        ) -> Result<Vec<AiTabletHit>, AiRetrievalError> {
            control
                .checkpoint()
                .map_err(|_| AiRetrievalError::Cancelled(control.reason()))?;
            assert_eq!(request.authorization_context, b"signed-user");
            Ok(vec![AiTabletHit {
                candidate: LocalCandidate {
                    tablet_id: request.tablet_id,
                    row_id: RowId(u64::from(request.tablet_id.as_bytes()[15])),
                    score: 0.9,
                    local_rank: 1,
                    rls_visible: true,
                },
                cells: vec![(1, Value::Int64(7))],
                exact_rerank_score: Some(0.99),
                consistency: None,
                contributions: Vec::new(),
                metadata: TabletAiResponseMetadata::default(),
            }])
        }
    }

    /// Multi-retriever hybrid executor: each tablet returns per-retriever
    /// contributions so the production path must call merge_hybrid_contributions.
    struct HybridExecutor;

    #[async_trait::async_trait]
    impl AiTabletExecutor for HybridExecutor {
        async fn retrieve(
            &self,
            request: &AiTabletQuery,
            control: ExecutionControl,
        ) -> Result<Vec<AiTabletHit>, AiRetrievalError> {
            control
                .checkpoint()
                .map_err(|_| AiRetrievalError::Cancelled(control.reason()))?;
            let tablet = request.tablet_id;
            // Tablet 1: dense ranks row 10 first; sparse ranks row 20 first.
            // Tablet 2: dense ranks row 20 first; sparse ranks row 10 first.
            // Global hybrid RRF should prefer row 10 (dense#1@t1 + sparse#1@t2).
            let is_t1 = tablet.as_bytes()[15] == 1;
            let (dense_row, sparse_row, dense_rank_10, sparse_rank_10) = if is_t1 {
                (10u64, 20u64, 1u32, 2u32)
            } else {
                (20u64, 10u64, 2u32, 1u32)
            };
            let mut hits = Vec::new();
            for &(row, dense_rank, sparse_rank) in &[
                (10u64, dense_rank_10, sparse_rank_10),
                (
                    20u64,
                    if dense_row == 20 { 1 } else { 2 },
                    if sparse_row == 20 { 1 } else { 2 },
                ),
            ] {
                let contributions = vec![
                    LocalRetrieverContribution {
                        tablet_id: tablet,
                        row_id: RowId(row),
                        retriever_id: "dense".into(),
                        retriever_kind: Some("ann".into()),
                        local_rank: dense_rank,
                        raw_score: 1.0 / f64::from(dense_rank),
                        upper_bound_after: None,
                        rls_visible: true,
                        weight: 1.0,
                    },
                    LocalRetrieverContribution {
                        tablet_id: tablet,
                        row_id: RowId(row),
                        retriever_id: "sparse".into(),
                        retriever_kind: Some("sparse".into()),
                        local_rank: sparse_rank,
                        raw_score: 1.0 / f64::from(sparse_rank),
                        upper_bound_after: None,
                        rls_visible: true,
                        weight: 1.0,
                    },
                ];
                hits.push(AiTabletHit {
                    candidate: LocalCandidate {
                        tablet_id: tablet,
                        row_id: RowId(row),
                        score: contributions.iter().map(|c| c.raw_score).sum(),
                        local_rank: dense_rank.min(sparse_rank),
                        rls_visible: true,
                    },
                    cells: vec![(1, Value::Int64(row as i64))],
                    exact_rerank_score: None,
                    consistency: None,
                    contributions,
                    metadata: TabletAiResponseMetadata {
                        retriever_exhausted: true,
                        model_generation: Some(1),
                        ..TabletAiResponseMetadata::default()
                    },
                });
            }
            Ok(hits)
        }
    }

    #[tokio::test]
    async fn remote_ai_fanout_merges_and_preserves_exact_rerank_payload() {
        let endpoint = Arc::new(RemoteAiEndpoint::new(Arc::new(StaticExecutor)));
        let client: Arc<dyn AiRpcClient> =
            Arc::new(LoopbackAiRpcClient::new(Arc::clone(&endpoint)));
        let transport = RemoteAiTransport::new(client);
        let budget = AiWorkBudget {
            candidate_ceiling: 2,
            max_local_candidates: 8,
            ..AiWorkBudget::default()
        };
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: Some(vec![1]),
        };
        let result = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(2), tid(1)],
                table: "items",
                search: &search,
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 2.0,
                budget: &budget,
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap();
        assert_eq!(result.candidates.len(), 2);
        assert_eq!(result.candidates[0].tablet_id, tid(1));
        assert_eq!(result.hits[0].exact_rerank_score, Some(0.99));
        assert_eq!(result.hits[0].cells, vec![(1, Value::Int64(7))]);
        assert_eq!(endpoint.active_executions(), 0);
    }

    #[test]
    fn fuse_distributed_hits_production_path_uses_hybrid_for_multi_retriever() {
        // Production path (not unit-only merge_hybrid_contributions): when
        // multiple named retrievers are present, fuse_distributed_hits must
        // sum per-retriever RRF contributions globally.
        let hits = vec![
            AiTabletHit {
                candidate: LocalCandidate {
                    tablet_id: tid(1),
                    row_id: RowId(10),
                    score: 0.5,
                    local_rank: 2, // wrong if used alone
                    rls_visible: true,
                },
                cells: vec![],
                exact_rerank_score: None,
                consistency: None,
                contributions: vec![
                    LocalRetrieverContribution::new(tid(1), RowId(10), "dense", 1, 0.9),
                    LocalRetrieverContribution::new(tid(1), RowId(10), "sparse", 1, 0.8),
                ],
                metadata: TabletAiResponseMetadata::default(),
            },
            AiTabletHit {
                candidate: LocalCandidate {
                    tablet_id: tid(2),
                    row_id: RowId(20),
                    score: 0.99,
                    local_rank: 1,
                    rls_visible: true,
                },
                cells: vec![],
                exact_rerank_score: None,
                consistency: None,
                contributions: vec![
                    LocalRetrieverContribution::new(tid(2), RowId(20), "dense", 2, 0.5),
                    LocalRetrieverContribution::new(tid(2), RowId(20), "sparse", 2, 0.4),
                ],
                metadata: TabletAiResponseMetadata::default(),
            },
        ];
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![
                mongreldb_core::query::NamedRetriever {
                    name: "dense".into(),
                    weight: 1.0,
                    retriever: Retriever::Ann {
                        column_id: 1,
                        query: vec![0.0],
                        k: 10,
                    },
                },
                mongreldb_core::query::NamedRetriever {
                    name: "sparse".into(),
                    weight: 1.0,
                    retriever: Retriever::Sparse {
                        column_id: 2,
                        query: vec![],
                        k: 10,
                    },
                },
            ],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: None,
        };
        let budget = AiWorkBudget {
            candidate_ceiling: 2,
            max_local_candidates: 16,
            ..AiWorkBudget::default()
        };
        let fused =
            fuse_distributed_hits(&hits, &search, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        assert_eq!(fused.len(), 2);
        // Row 10 has two rank-1 components → 2/61; row 20 has two rank-2 → 2/62.
        assert_eq!(fused[0].row_id, RowId(10));
        assert!((fused[0].final_score - 2.0 / 61.0).abs() < 1e-12);
        // Single-score merge_candidates would have preferred row 20 (local_rank 1).
        let single = merge_candidates(
            &hits.iter().map(|h| h.candidate.clone()).collect::<Vec<_>>(),
            FusionMethod::Rrf { k: 60 },
            &budget,
        )
        .unwrap();
        assert_eq!(
            single[0].row_id,
            RowId(20),
            "sanity: single-rank merge prefers wrong winner"
        );
        assert_ne!(
            fused[0].row_id, single[0].row_id,
            "production hybrid path must differ from single local_rank RRF"
        );
    }

    #[tokio::test]
    async fn remote_ai_transport_production_path_hybrid_fuse_multi_retriever() {
        let endpoint = Arc::new(RemoteAiEndpoint::new(Arc::new(HybridExecutor)));
        let client: Arc<dyn AiRpcClient> =
            Arc::new(LoopbackAiRpcClient::new(Arc::clone(&endpoint)));
        let transport = RemoteAiTransport::new(client);
        let budget = AiWorkBudget {
            candidate_ceiling: 2,
            max_local_candidates: 32,
            ..AiWorkBudget::default()
        };
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![
                mongreldb_core::query::NamedRetriever {
                    name: "dense".into(),
                    weight: 1.0,
                    retriever: Retriever::Ann {
                        column_id: 1,
                        query: vec![0.0],
                        k: 10,
                    },
                },
                mongreldb_core::query::NamedRetriever {
                    name: "sparse".into(),
                    weight: 1.0,
                    retriever: Retriever::Sparse {
                        column_id: 2,
                        query: vec![],
                        k: 10,
                    },
                },
            ],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: Some(vec![1]),
        };
        let result = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(1), tid(2)],
                table: "items",
                search: &search,
                authorization_context: b"",
                fusion: FusionMethod::Rrf { k: 60 },
                // ceil(2 * 2.0 / 2) = 2 local hits per tablet.
                overfetch_factor: 2.0,
                budget: &budget,
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap();
        assert!(!result.candidates.is_empty());
        // Global hybrid prefers row 10 (dense@t1 rank1 + sparse@t2 rank1).
        assert_eq!(result.candidates[0].row_id, RowId(10));
        assert_eq!(endpoint.active_executions(), 0);
    }

    #[tokio::test]
    async fn remote_ai_enforces_deadline_and_retained_memory_budget() {
        let endpoint = Arc::new(RemoteAiEndpoint::new(Arc::new(StaticExecutor)));
        let client: Arc<dyn AiRpcClient> =
            Arc::new(LoopbackAiRpcClient::new(Arc::clone(&endpoint)));
        let transport = RemoteAiTransport::new(client);
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 1,
            projection: Some(vec![1]),
        };
        let deadline = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(1)],
                table: "items",
                search: &search,
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 1.0,
                budget: &AiWorkBudget {
                    deadline_ms: 0,
                    ..AiWorkBudget::default()
                },
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap_err();
        assert_eq!(
            deadline,
            AiRetrievalError::Cancelled(CancellationReason::Deadline)
        );

        let memory = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(1)],
                table: "items",
                search: &search,
                authorization_context: b"signed-user",
                fusion: FusionMethod::default(),
                overfetch_factor: 1.0,
                budget: &AiWorkBudget {
                    memory_bytes: 1,
                    ..AiWorkBudget::default()
                },
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap_err();
        assert!(matches!(memory, AiRetrievalError::BudgetExceeded(_)));
        assert_eq!(endpoint.active_executions(), 0);
    }

    // -----------------------------------------------------------------------
    // Global hybrid fusion (P0.8)
    // -----------------------------------------------------------------------

    fn contrib(
        tablet: u8,
        row: u64,
        retriever: &str,
        rank: u32,
        score: f64,
    ) -> LocalRetrieverContribution {
        LocalRetrieverContribution::new(tid(tablet), RowId(row), retriever, rank, score)
    }

    #[test]
    fn merge_hybrid_global_rrf_sums_components_and_returns_provenance() {
        // Row 10: dense rank 1 + sparse rank 2 → 1/61 + 1/62
        // Row 20: dense rank 2 only → 1/62
        // Row 30: sparse rank 1 only → 1/61
        let contributions = vec![
            contrib(1, 10, "dense", 1, 0.99),
            contrib(2, 10, "sparse", 2, 0.40),
            contrib(1, 20, "dense", 2, 0.80),
            contrib(2, 30, "sparse", 1, 0.95),
        ];
        let budget = AiWorkBudget {
            candidate_ceiling: 10,
            ..AiWorkBudget::default()
        };
        let result =
            merge_hybrid_contributions(&contributions, FusionMethod::Rrf { k: 60 }, &budget, &[])
                .unwrap();
        assert_eq!(result.candidates.len(), 3);
        // Highest fused: row 10 (two components).
        assert_eq!(result.candidates[0].row_id, RowId(10));
        assert_eq!(result.candidates[0].components.len(), 2);
        let expected = 1.0 / 61.0 + 1.0 / 62.0;
        assert!((result.candidates[0].final_score - expected).abs() < 1e-12);
        // Component scores present and named.
        let names: Vec<_> = result.candidates[0]
            .components
            .iter()
            .map(|c| c.retriever_id.as_str())
            .collect();
        assert_eq!(names, vec!["dense", "sparse"]);
        // Rows 20 and 30 share the same single-component RRF (rank 2 dense vs
        // rank 1 sparse differ): rank-1 sparse (1/61) > rank-2 dense (1/62).
        assert_eq!(result.candidates[1].row_id, RowId(30));
        assert_eq!(result.candidates[2].row_id, RowId(20));
        assert!(result.refill.is_empty());
    }

    #[test]
    fn merge_hybrid_dedups_duplicate_retriever_contributions() {
        let contributions = vec![
            contrib(1, 7, "dense", 2, 0.5),
            contrib(1, 7, "dense", 1, 0.9), // better rank wins
            contrib(2, 7, "dense", 3, 0.4), // worse
        ];
        let out = merge_hybrid_contributions(
            &contributions,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget::default(),
            &[],
        )
        .unwrap();
        assert_eq!(out.candidates.len(), 1);
        assert_eq!(out.candidates[0].components.len(), 1);
        assert_eq!(out.candidates[0].components[0].rank, 1);
        assert!((out.candidates[0].final_score - 1.0 / 61.0).abs() < 1e-12);
    }

    #[test]
    fn merge_hybrid_retriever_order_does_not_alter_output() {
        let a = vec![
            contrib(1, 1, "dense", 1, 1.0),
            contrib(1, 1, "sparse", 2, 0.5),
            contrib(2, 2, "minhash", 1, 0.8),
        ];
        let mut b = a.clone();
        b.reverse();
        let budget = AiWorkBudget::default();
        let ra = merge_hybrid_contributions(&a, FusionMethod::Rrf { k: 60 }, &budget, &[]).unwrap();
        let rb = merge_hybrid_contributions(&b, FusionMethod::Rrf { k: 60 }, &budget, &[]).unwrap();
        assert_eq!(ra.candidates, rb.candidates);
    }

    #[test]
    fn merge_hybrid_rls_hidden_fails_closed() {
        let mut c = contrib(1, 1, "dense", 1, 1.0);
        c.rls_visible = false;
        let err = merge_hybrid_contributions(
            &[c],
            FusionMethod::default(),
            &AiWorkBudget::default(),
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, AiRetrievalError::RlsHygiene { .. }));
    }

    // ID: P0.8-X6 Adaptive refill recovers a global winner.
    #[test]
    fn hybrid_adaptive_refill_recovers_global_winner() {
        // Initial: tablet 1 only has a rank-2 dense hit (RRF 1/62). Tablet 2 is
        // not exhausted and can still emit a rank-1 hit (RRF 1/61) that must
        // displace the provisional winner after refill.
        let initial = vec![contrib(1, 1, "dense", 2, 0.5)];
        let mut bounds = HashMap::new();
        bounds.insert(
            (tid(1), "dense".to_owned()),
            RetrieverTabletBound::exhausted(tid(1), "dense"),
        );
        bounds.insert(
            (tid(2), "dense".to_owned()),
            RetrieverTabletBound::open(tid(2), "dense", 1, Some(1.0), 1.0),
        );
        let budget = AiWorkBudget {
            candidate_ceiling: 1,
            max_local_candidates: 100,
            ..AiWorkBudget::default()
        };
        let mut refill_calls = 0u32;
        let winners = exact_hybrid_merge(
            initial,
            bounds,
            FusionMethod::Rrf { k: 60 },
            &budget,
            1,
            |tablet, retriever| {
                refill_calls += 1;
                assert_eq!(tablet, tid(2));
                assert_eq!(retriever, "dense");
                Ok((
                    vec![contrib(2, 99, "dense", 1, 0.99)],
                    RetrieverTabletBound::exhausted(tid(2), "dense"),
                ))
            },
        )
        .unwrap();
        assert_eq!(refill_calls, 1);
        assert_eq!(winners.len(), 1);
        assert_eq!(winners[0].row_id, RowId(99));
        assert_eq!(winners[0].tablet_id, tid(2));
        assert_eq!(winners[0].components.len(), 1);
        assert!((winners[0].final_score - 1.0 / 61.0).abs() < 1e-12);
    }

    // ID: P0.8-X7 Exact global rerank changes order correctly.
    #[test]
    fn exact_global_rerank_changes_order_correctly() {
        // Fused RRF alone prefers row 1 (rank 1). Exact scores invert: row 2
        // has a much higher exact score, so global rerank must place it first.
        let hits = vec![
            AiTabletHit {
                candidate: cand(1, 1, 0.9, 1),
                cells: vec![],
                exact_rerank_score: Some(0.1),
                consistency: None,
                contributions: vec![LocalRetrieverContribution::new(
                    tid(1),
                    RowId(1),
                    "dense",
                    1,
                    0.9,
                )],
                metadata: TabletAiResponseMetadata::default(),
            },
            AiTabletHit {
                candidate: cand(1, 2, 0.5, 2),
                cells: vec![],
                exact_rerank_score: Some(0.95),
                consistency: None,
                contributions: vec![LocalRetrieverContribution::new(
                    tid(1),
                    RowId(2),
                    "dense",
                    2,
                    0.5,
                )],
                metadata: TabletAiResponseMetadata::default(),
            },
        ];
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![mongreldb_core::query::NamedRetriever {
                name: "dense".into(),
                weight: 1.0,
                retriever: Retriever::Ann {
                    column_id: 1,
                    query: vec![1.0],
                    k: 2,
                },
            }],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: Some(Rerank::ExactVector {
                embedding_column: 1,
                query: vec![1.0],
                metric: mongreldb_core::query::VectorMetric::Cosine,
                candidate_limit: 2,
                weight: 1.0,
            }),
            limit: 2,
            projection: None,
        };
        let fused = fuse_distributed_hits(
            &hits,
            &search,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget::default(),
        )
        .unwrap();
        assert_eq!(fused.len(), 2);
        assert_eq!(
            fused[0].row_id,
            RowId(2),
            "exact rerank must promote row 2 over fused rank-1: {fused:?}"
        );
        assert_eq!(fused[1].row_id, RowId(1));
        // final = RRF(2) + 0.95 = 1/62 + 0.95  >  RRF(1) + 0.1 = 1/61 + 0.1
        assert!(fused[0].final_score > fused[1].final_score);
    }

    #[test]
    fn hybrid_refill_targets_skips_exhausted_and_dominated_streams() {
        let winners = vec![HybridMergedCandidate {
            tablet_id: tid(1),
            row_id: RowId(1),
            final_score: 1.0 / 61.0,
            raw_score: 1.0,
            components: Vec::new(),
        }];
        let bounds = vec![
            RetrieverTabletBound::exhausted(tid(1), "dense"),
            // next rank 1000 → contribution ~1/1060 << 1/61 → no refill
            RetrieverTabletBound::open(tid(2), "dense", 1000, Some(0.1), 1.0),
            // next rank 1 → contribution 1/61 == threshold → refill
            RetrieverTabletBound::open(tid(3), "sparse", 1, Some(1.0), 1.0),
        ];
        let refill = hybrid_refill_targets(&winners, 1, FusionMethod::Rrf { k: 60 }, &bounds);
        assert_eq!(refill, vec![(tid(3), "sparse".to_owned())]);
    }

    #[test]
    fn tablet_ai_response_metadata_round_trips() {
        let meta = TabletAiResponseMetadata {
            retriever_exhausted: true,
            unseen_score_bound: Some(0.42),
            index_applied_hlc: Some("100.0.1".into()),
            read_hlc: Some("101.2.3".into()),
            model_generation: Some(7),
        };
        let hit = AiTabletHit {
            candidate: cand(1, 1, 1.0, 1),
            cells: vec![(1, Value::Int64(1))],
            exact_rerank_score: None,
            consistency: None,
            contributions: Vec::new(),
            metadata: meta.clone(),
        };
        let bytes = bincode::serialize(&hit).unwrap();
        let restored: AiTabletHit = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.metadata, meta);
        assert!(restored.metadata.retriever_exhausted);
        assert_eq!(restored.metadata.unseen_score_bound, Some(0.42));
        assert_eq!(restored.metadata.model_generation, Some(7));
        assert_eq!(
            restored.metadata.index_applied_hlc.as_deref(),
            Some("100.0.1")
        );
        assert_eq!(restored.metadata.read_hlc.as_deref(), Some("101.2.3"));
    }

    #[test]
    fn fuse_fail_closed_on_model_generation_mismatch() {
        let mut hit_a = AiTabletHit {
            candidate: cand(1, 10, 0.9, 1),
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: vec![LocalRetrieverContribution::new(
                tid(1),
                RowId(10),
                "dense",
                1,
                0.9,
            )],
            metadata: TabletAiResponseMetadata {
                model_generation: Some(1),
                ..TabletAiResponseMetadata::default()
            },
        };
        let hit_b = AiTabletHit {
            candidate: cand(2, 20, 0.8, 1),
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: vec![LocalRetrieverContribution::new(
                tid(2),
                RowId(20),
                "dense",
                1,
                0.8,
            )],
            metadata: TabletAiResponseMetadata {
                model_generation: Some(2), // mismatch
                ..TabletAiResponseMetadata::default()
            },
        };
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![mongreldb_core::query::NamedRetriever {
                name: "dense".into(),
                weight: 1.0,
                retriever: Retriever::Ann {
                    column_id: 1,
                    query: vec![0.0],
                    k: 10,
                },
            }],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: None,
        };
        let err = fuse_distributed_hits(
            &[hit_a.clone(), hit_b],
            &search,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget::default(),
        )
        .unwrap_err();
        assert!(
            matches!(err, AiRetrievalError::Protocol(ref msg) if msg.contains("model generation")),
            "unexpected: {err:?}"
        );

        // expected_model_generation on budget also fail-closes.
        hit_a.metadata.model_generation = Some(9);
        let budget = AiWorkBudget {
            expected_model_generation: Some(1),
            ..AiWorkBudget::default()
        };
        let err = fuse_distributed_hits(&[hit_a], &search, FusionMethod::Rrf { k: 60 }, &budget)
            .unwrap_err();
        assert!(
            matches!(err, AiRetrievalError::Protocol(ref msg) if msg.contains("expected 1")),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn ai_work_budget_parent_fields_have_defaults() {
        let b = AiWorkBudget::default();
        assert!(b.max_tablet_rpcs > 0);
        assert!(b.max_retained_contributions > 0);
        assert!(b.max_refill_rounds > 0);
        assert!(b.max_payload_cells > 0);
        assert!(b.max_exact_gathers > 0);
        assert!(b.max_rerank_dimensions > 0);
        assert!(b.max_serialization_bytes > 0);
        assert!(b.expected_model_generation.is_none());
    }

    // P0.8-X2: Dense + Sparse + MinHash global RRF matches centralized baseline.
    #[test]
    fn p08_x2_dense_sparse_minhash_global_rrf_matches_baseline() {
        // Centralized baseline: one contribution per named retriever per row.
        let contributions = vec![
            contrib(1, 10, "dense", 1, 0.99),
            contrib(1, 10, "sparse", 2, 0.40),
            contrib(2, 10, "minhash", 1, 0.90),
            contrib(1, 20, "dense", 2, 0.80),
            contrib(2, 20, "sparse", 1, 0.95),
            contrib(1, 20, "minhash", 2, 0.50),
        ];
        let budget = AiWorkBudget {
            candidate_ceiling: 10,
            ..AiWorkBudget::default()
        };
        let hybrid =
            merge_hybrid_contributions(&contributions, FusionMethod::Rrf { k: 60 }, &budget, &[])
                .unwrap();
        // Row 10: rank1 dense + rank2 sparse + rank1 minhash = 1/61 + 1/62 + 1/61
        // Row 20: rank2 dense + rank1 sparse + rank2 minhash = 1/62 + 1/61 + 1/62
        assert_eq!(hybrid.candidates[0].row_id, RowId(10));
        let expected_10 = 1.0 / 61.0 + 1.0 / 62.0 + 1.0 / 61.0;
        assert!((hybrid.candidates[0].final_score - expected_10).abs() < 1e-12);
        assert_eq!(hybrid.candidates[0].components.len(), 3);
        let names: Vec<_> = hybrid.candidates[0]
            .components
            .iter()
            .map(|c| c.retriever_id.as_str())
            .collect();
        assert_eq!(names, vec!["dense", "minhash", "sparse"]);

        // Production fuse path with MinHash retriever kind present.
        let hits = vec![AiTabletHit {
            candidate: cand(1, 10, 1.0, 1),
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: vec![
                LocalRetrieverContribution::new(tid(1), RowId(10), "dense", 1, 0.99),
                LocalRetrieverContribution::new(tid(1), RowId(10), "sparse", 2, 0.40),
                LocalRetrieverContribution::new(tid(1), RowId(10), "minhash", 1, 0.90),
            ],
            metadata: TabletAiResponseMetadata {
                model_generation: Some(1),
                ..TabletAiResponseMetadata::default()
            },
        }];
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![
                mongreldb_core::query::NamedRetriever {
                    name: "dense".into(),
                    weight: 1.0,
                    retriever: Retriever::Ann {
                        column_id: 1,
                        query: vec![0.0],
                        k: 10,
                    },
                },
                mongreldb_core::query::NamedRetriever {
                    name: "sparse".into(),
                    weight: 1.0,
                    retriever: Retriever::Sparse {
                        column_id: 2,
                        query: vec![],
                        k: 10,
                    },
                },
                mongreldb_core::query::NamedRetriever {
                    name: "minhash".into(),
                    weight: 1.0,
                    retriever: Retriever::MinHash {
                        column_id: 3,
                        members: vec![],
                        k: 10,
                    },
                },
            ],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 1,
            projection: None,
        };
        let fused =
            fuse_distributed_hits(&hits, &search, FusionMethod::Rrf { k: 60 }, &budget).unwrap();
        assert_eq!(fused[0].row_id, RowId(10));
        assert!((fused[0].final_score - expected_10).abs() < 1e-12);
    }

    // P0.8-X5: RLS-hidden rows never contribute (candidate + contribution paths).
    #[test]
    fn p08_x5_rls_hidden_rows_never_contribute() {
        let mut c = cand(1, 1, 1.0, 1);
        c.rls_visible = false;
        let err =
            merge_candidates(&[c], FusionMethod::default(), &AiWorkBudget::default()).unwrap_err();
        assert!(matches!(err, AiRetrievalError::RlsHygiene { .. }));

        let mut contrib_hidden = contrib(1, 1, "dense", 1, 1.0);
        contrib_hidden.rls_visible = false;
        let err = merge_hybrid_contributions(
            &[contrib_hidden],
            FusionMethod::default(),
            &AiWorkBudget::default(),
            &[],
        )
        .unwrap_err();
        assert!(matches!(err, AiRetrievalError::RlsHygiene { .. }));

        // Endpoint path also fails closed on rls_visible=false hits.
        let hit = AiTabletHit {
            candidate: {
                let mut c = cand(1, 9, 1.0, 1);
                c.rls_visible = false;
                c
            },
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: Vec::new(),
            metadata: TabletAiResponseMetadata::default(),
        };
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 1,
            projection: None,
        };
        let err = fuse_distributed_hits(
            &[hit],
            &search,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget::default(),
        )
        .unwrap_err();
        assert!(matches!(err, AiRetrievalError::RlsHygiene { .. }));
    }

    // P0.8-X9: model/index generation mismatch fails closed.
    #[test]
    fn p08_x9_model_generation_mismatch_fails() {
        let hit_a = AiTabletHit {
            candidate: cand(1, 10, 0.9, 1),
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: vec![LocalRetrieverContribution::new(
                tid(1),
                RowId(10),
                "dense",
                1,
                0.9,
            )],
            metadata: TabletAiResponseMetadata {
                model_generation: Some(1),
                ..TabletAiResponseMetadata::default()
            },
        };
        let hit_b = AiTabletHit {
            candidate: cand(2, 20, 0.8, 1),
            cells: vec![],
            exact_rerank_score: None,
            consistency: None,
            contributions: vec![LocalRetrieverContribution::new(
                tid(2),
                RowId(20),
                "dense",
                1,
                0.8,
            )],
            metadata: TabletAiResponseMetadata {
                model_generation: Some(2),
                ..TabletAiResponseMetadata::default()
            },
        };
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: vec![mongreldb_core::query::NamedRetriever {
                name: "dense".into(),
                weight: 1.0,
                retriever: Retriever::Ann {
                    column_id: 1,
                    query: vec![0.0],
                    k: 10,
                },
            }],
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: None,
        };
        let err = fuse_distributed_hits(
            &[hit_a, hit_b],
            &search,
            FusionMethod::Rrf { k: 60 },
            &AiWorkBudget::default(),
        )
        .unwrap_err();
        assert!(
            matches!(err, AiRetrievalError::Protocol(ref msg) if msg.contains("model generation")),
            "unexpected: {err:?}"
        );
    }

    /// Failing executor for one tablet — used by P0.8-X10.
    struct FailingTabletExecutor {
        fail_tablet: u8,
    }

    #[async_trait::async_trait]
    impl AiTabletExecutor for FailingTabletExecutor {
        async fn retrieve(
            &self,
            request: &AiTabletQuery,
            control: ExecutionControl,
        ) -> Result<Vec<AiTabletHit>, AiRetrievalError> {
            control
                .checkpoint()
                .map_err(|_| AiRetrievalError::Cancelled(control.reason()))?;
            let n = request.tablet_id.as_bytes()[15];
            if n == self.fail_tablet {
                return Err(AiRetrievalError::Transport(format!(
                    "tablet {n} unavailable"
                )));
            }
            Ok(vec![AiTabletHit {
                candidate: LocalCandidate {
                    tablet_id: request.tablet_id,
                    row_id: RowId(u64::from(n)),
                    score: 0.9,
                    local_rank: 1,
                    rls_visible: true,
                },
                cells: vec![],
                exact_rerank_score: None,
                consistency: None,
                contributions: Vec::new(),
                metadata: TabletAiResponseMetadata::default(),
            }])
        }
    }

    // P0.8-X10: one tablet failure is fail-closed (no partial winners).
    #[tokio::test]
    async fn p08_x10_one_tablet_failure_fail_closed() {
        let endpoint = Arc::new(RemoteAiEndpoint::new(Arc::new(FailingTabletExecutor {
            fail_tablet: 2,
        })));
        let client: Arc<dyn AiRpcClient> =
            Arc::new(LoopbackAiRpcClient::new(Arc::clone(&endpoint)));
        let transport = RemoteAiTransport::new(client);
        let search = SearchRequest {
            must: Vec::new(),
            retrievers: Vec::new(),
            fusion: Fusion::ReciprocalRank { constant: 60 },
            rerank: None,
            limit: 2,
            projection: None,
        };
        let err = transport
            .retrieve(AiFanoutRequest {
                query_id: QueryId::new_random(),
                tablets: &[tid(1), tid(2)],
                table: "items",
                search: &search,
                authorization_context: b"",
                fusion: FusionMethod::default(),
                overfetch_factor: 1.0,
                budget: &AiWorkBudget::default(),
                control: &ExecutionControl::new(None),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, AiRetrievalError::Transport(ref msg) if msg.contains("unavailable")),
            "fail-closed on tablet failure, got: {err:?}"
        );
        assert_eq!(endpoint.active_executions(), 0);
    }
}
