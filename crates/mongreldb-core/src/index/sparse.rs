//! Sparse inverted index for SPLADE-style learned-sparse retrieval.
//!
//! Each document is a sparse vector `(token → weight)`; the index is an inverted
//! list `token → [(row_id, weight)]`. A query (also a sparse vector) scores
//! documents by sparse dot product over shared tokens and returns the top-k.
//! Real SPLADE produces these sparse vectors from a trained model; here any
//! tokenizer works (the demo uses a hashing trick), so the retrieval machinery
//! is model-agnostic — plug in real SPLADE weights as the sparse vectors.
//!
//! Like the other indexes, results resolve to the shared [`crate::rowid::RowId`]
//! space, so `sparse_match ∩ fm_contains ∩ bitmap_eq` composes in one query.

use crate::rowid::RowId;
use crate::Result;
use std::collections::HashMap;

/// Inverted index over weighted sparse vectors, keyed by token id.
#[derive(Clone)]
pub struct SparseIndex {
    postings: HashMap<u32, Vec<(RowId, f32)>>,
}

impl SparseIndex {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
        }
    }

    /// Insert a document's sparse vector (`terms` need not be sorted; duplicate
    /// tokens within one doc accumulate).
    pub fn insert(&mut self, terms: &[(u32, f32)], row_id: RowId) {
        for &(token, weight) in terms {
            self.postings
                .entry(token)
                .or_default()
                .push((row_id, weight));
        }
    }

    /// Top-k row ids by sparse dot product with `query` (highest score first).
    pub fn search(&self, query: &[(u32, f32)], k: usize) -> Vec<(RowId, f64)> {
        self.search_filtered(query, k, |_| true)
    }

    pub fn search_filtered(
        &self,
        query: &[(u32, f32)],
        k: usize,
        allowed: impl Fn(RowId) -> bool,
    ) -> Vec<(RowId, f64)> {
        let mut scores: HashMap<u64, f64> = HashMap::new();
        for &(token, q_weight) in query {
            if let Some(list) = self.postings.get(&token) {
                for &(rid, d_weight) in list {
                    if allowed(rid) {
                        *scores.entry(rid.0).or_insert(0.0) +=
                            f64::from(q_weight) * f64::from(d_weight);
                    }
                }
            }
        }
        let mut ranked: Vec<(RowId, f64)> = scores
            .into_iter()
            .map(|(rid, score)| (RowId(rid), score))
            .collect();
        ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked.truncate(k);
        ranked
    }

    pub fn search_with_context(
        &self,
        query: &[(u32, f32)],
        k: usize,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<(RowId, f64)>> {
        let mut scores: HashMap<u64, f64> = HashMap::new();
        for &(token, q_weight) in query {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            if let Some(list) = self.postings.get(&token) {
                for chunk in list.chunks(256) {
                    if let Some(context) = context {
                        context.consume(chunk.len())?;
                    }
                    for &(rid, d_weight) in chunk {
                        if !scores.contains_key(&rid.0)
                            && scores.len() >= crate::query::MAX_RAW_INDEX_CANDIDATES
                        {
                            return Err(crate::MongrelError::WorkBudgetExceeded);
                        }
                        *scores.entry(rid.0).or_insert(0.0) +=
                            f64::from(q_weight) * f64::from(d_weight);
                    }
                }
            }
        }
        let mut ranked: Vec<_> = scores
            .into_iter()
            .map(|(rid, score)| (RowId(rid), score))
            .collect();
        if let Some(context) = context {
            context.consume(ranked.len())?;
        }
        let order = |left: &(RowId, f64), right: &(RowId, f64)| {
            right
                .1
                .total_cmp(&left.1)
                .then_with(|| left.0.cmp(&right.0))
        };
        if ranked.len() > k {
            ranked.select_nth_unstable_by(k, order);
            ranked.truncate(k);
        }
        ranked.sort_by(order);
        Ok(ranked)
    }

    pub fn candidate_row_ids(&self, query: &[(u32, f32)]) -> Vec<RowId> {
        let mut row_ids = std::collections::HashSet::new();
        for (token, _) in query {
            if let Some(postings) = self.postings.get(token) {
                row_ids.extend(postings.iter().map(|(row_id, _)| *row_id));
            }
        }
        row_ids.into_iter().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    /// Snapshot the inverted lists for checkpointing to `_idx/global.idx`.
    pub fn entries(&self) -> Vec<(u32, Vec<(RowId, f32)>)> {
        self.postings
            .iter()
            .map(|(t, list)| (*t, list.clone()))
            .collect()
    }

    /// Rebuild from a snapshot produced by [`SparseIndex::entries`].
    pub fn from_entries(entries: Vec<(u32, Vec<(RowId, f32)>)>) -> Self {
        let mut postings = HashMap::new();
        for (t, list) in entries {
            postings.insert(t, list);
        }
        Self { postings }
    }
}

impl Default for SparseIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranks_by_sparse_overlap() {
        let mut idx = SparseIndex::new();
        // doc 0: {a:2, b:1}; doc 1: {a:1, c:3}; doc 2: {b:5}
        idx.insert(&[(1, 2.0), (2, 1.0)], RowId(0));
        idx.insert(&[(1, 1.0), (3, 3.0)], RowId(1));
        idx.insert(&[(2, 5.0)], RowId(2));
        // query {a:1, b:1}: doc0 = 2*1+1*1=3, doc1 = 1*1=1, doc2 = 5*1=5
        let top = idx.search(&[(1, 1.0), (2, 1.0)], 3);
        assert_eq!(top[0], (RowId(2), 5.0));
        assert_eq!(top[1], (RowId(0), 3.0));
        assert_eq!(top[2], (RowId(1), 1.0));
    }

    #[test]
    fn unique_candidates_stop_at_raw_ceiling() {
        let mut idx = SparseIndex::new();
        for row_id in 0..=crate::query::MAX_RAW_INDEX_CANDIDATES {
            idx.insert(&[(1, 1.0)], RowId(row_id as u64));
        }
        let context = crate::query::AiExecutionContext::new(None, usize::MAX);
        assert!(matches!(
            idx.search_with_context(&[(1, 1.0)], 1, Some(&context)),
            Err(crate::MongrelError::WorkBudgetExceeded)
        ));
    }
}
