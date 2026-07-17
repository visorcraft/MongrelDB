//! Binary-quantized ANN index — the AI-native semantic access path.
//!
//! Vectors are quantized to 1 bit/dim (`sign(v)`), so a 768-dim embedding is
//! 96 bytes/row and similarity is Hamming distance via SIMD `popcount(XOR)`.
//! Search uses a real **HNSW** graph ([`crate::index::hnsw::Hnsw`]); an agent
//! composes `semsearch(text, k)` with the other row-id-space primitives.
//!
//! S1C-003 base+delta layout: the index is an immutable **base** HNSW graph
//! (the single consolidated frozen layer) plus zero or more immutable frozen
//! delta graphs plus one small active mutable delta graph. Search runs
//! per-layer HNSW candidate lists, merges them keeping each row's exact
//! minimum Hamming distance (exact rerank over the quantized vectors), and
//! truncates to `k` — so recall is independent of how the rows are split
//! across layers. [`AnnIndex::merge_deltas_into_base`] is the compaction
//! step: it merges every frozen delta into a new base graph. Deleted rows
//! keep stale graph entries (HNSW has no cheap node removal); readers apply
//! the visibility/tombstone filter via [`AnnIndex::search_filtered`].

use crate::index::hnsw::Hnsw;
use crate::rowid::RowId;
use crate::{MongrelError, Result};
use bincode::Options;
use std::collections::HashMap;
use std::sync::Arc;

const M: usize = 16;
const EF_CONSTRUCTION: usize = 64;
const EF_SEARCH: usize = 64;

/// Over-fetch multiplier for [`AnnIndex::search_filtered`]: each layer
/// contributes this times `k` candidates (at least `k + 16`, capped at the
/// layer length) so visible rows hidden behind tombstoned nearer neighbors
/// still make the merged candidate list.
const FILTERED_SEARCH_OVERFETCH: usize = 4;

/// Quantized vector store keyed by [`RowId`], backed by HNSW graphs in the
/// S1C-003 base+delta layout: `frozen` holds the immutable base (after
/// consolidation, a single layer) plus immutable frozen deltas, `active` is
/// the small mutable delta writers insert into.
#[derive(Clone)]
pub struct AnnIndex {
    dim: usize,
    bytes_per_vec: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    frozen: Arc<Vec<Arc<Hnsw>>>,
    active: Hnsw,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AnnCheckpoint {
    dim: usize,
    bytes_per_vec: usize,
    graph: Hnsw,
    ef_search: usize,
}

impl AnnIndex {
    pub fn new(dim: usize) -> Self {
        Self::with_options(dim, M, EF_CONSTRUCTION, EF_SEARCH)
    }

    pub fn with_options(dim: usize, m: usize, ef_construction: usize, ef_search: usize) -> Self {
        let bytes_per_vec = dim.div_ceil(8);
        Self {
            dim,
            bytes_per_vec,
            m,
            ef_construction,
            ef_search,
            frozen: Arc::new(Vec::new()),
            active: Hnsw::new(bytes_per_vec, m, ef_construction),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn ef_search(&self) -> usize {
        self.ef_search
    }

    /// Quantize an f32 vector to a packed bit vector (1 bit per dim, sign bit).
    pub fn quantize(&self, vec: &[f32]) -> Result<Vec<u8>> {
        if vec.len() != self.dim {
            return Err(MongrelError::InvalidArgument(format!(
                "embedding dimension must be {}, got {}",
                self.dim,
                vec.len()
            )));
        }
        if vec.iter().any(|value| !value.is_finite()) {
            return Err(MongrelError::InvalidArgument(
                "embedding values must be finite".into(),
            ));
        }
        let mut out = vec![0u8; self.bytes_per_vec];
        for (i, v) in vec.iter().enumerate() {
            if *v > 0.0 {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        Ok(out)
    }

    /// Insert a quantized vector for `row_id`.
    pub fn insert_quantized(&mut self, bits: Vec<u8>, row_id: RowId) -> Result<()> {
        if bits.len() != self.bytes_per_vec {
            return Err(MongrelError::InvalidArgument(format!(
                "quantized vector length must be {}, got {}",
                self.bytes_per_vec,
                bits.len()
            )));
        }
        self.active.insert(bits, row_id);
        Ok(())
    }

    /// Convenience: quantize then insert.
    pub fn insert(&mut self, vec: &[f32], row_id: RowId) -> Result<()> {
        let bits = self.quantize(vec)?;
        self.insert_quantized(bits, row_id)
    }

    pub(crate) fn insert_validated(&mut self, vec: &[f32], row_id: RowId) {
        if vec.len() != self.dim || vec.iter().any(|value| !value.is_finite()) {
            // Historical malformed rows are quarantined from the derived index.
            return;
        }
        let mut bits = vec![0u8; self.bytes_per_vec];
        for (i, value) in vec.iter().enumerate() {
            if *value > 0.0 {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        self.active.insert(bits, row_id);
    }

    /// k-nearest by Hamming distance.
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(RowId, u32)>> {
        self.search_with_context(query, k, None)
    }

    /// Base+delta search: each layer (base graph, frozen deltas, active
    /// delta) contributes its HNSW candidate list, the lists are merged
    /// keeping each row's exact minimum Hamming distance (exact rerank over
    /// the quantized vectors), and the merged list is truncated to `k`.
    pub fn search_with_context(
        &self,
        query: &[f32],
        k: usize,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<(RowId, u32)>> {
        let q = self.quantize(query)?;
        let mut best = HashMap::<RowId, u32>::new();
        for graph in self
            .frozen
            .iter()
            .map(Arc::as_ref)
            .chain(std::iter::once(&self.active))
        {
            for (row_id, distance) in graph.search_with_context(&q, k, self.ef_search, context)? {
                best.entry(row_id)
                    .and_modify(|current| *current = (*current).min(distance))
                    .or_insert(distance);
            }
        }
        let mut results: Vec<_> = best.into_iter().collect();
        results.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        results.truncate(k);
        Ok(results)
    }

    /// k-nearest by Hamming distance among rows accepted by `visible` — the
    /// S1C-003 visibility/tombstone filter applied across the base, frozen
    /// deltas, and active delta. Candidates are merged and reranked exactly
    /// as in [`Self::search_with_context`]; each layer is over-fetched (see
    /// [`FILTERED_SEARCH_OVERFETCH`]) so visible rows ranked behind filtered
    /// ones still surface.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        visible: &dyn Fn(RowId) -> bool,
    ) -> Result<Vec<(RowId, u32)>> {
        let q = self.quantize(query)?;
        let overfetch = k
            .saturating_mul(FILTERED_SEARCH_OVERFETCH)
            .max(k.saturating_add(16));
        let mut best = HashMap::<RowId, u32>::new();
        for graph in self
            .frozen
            .iter()
            .map(Arc::as_ref)
            .chain(std::iter::once(&self.active))
        {
            let fetch = overfetch.min(graph.len());
            for (row_id, distance) in graph.search(&q, fetch, self.ef_search.max(fetch)) {
                if !visible(row_id) {
                    continue;
                }
                best.entry(row_id)
                    .and_modify(|current| *current = (*current).min(distance))
                    .or_insert(distance);
            }
        }
        let mut results: Vec<_> = best.into_iter().collect();
        results.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));
        results.truncate(k);
        Ok(results)
    }

    pub fn len(&self) -> usize {
        self.active.len() + self.frozen.iter().map(|graph| graph.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.frozen.is_empty()
    }

    /// Checkpoint the whole HNSW graph (O(N) load, no O(N log N) rebuild).
    pub fn freeze(&self) -> Vec<u8> {
        let mut graph = Hnsw::new(self.bytes_per_vec, self.m, self.ef_construction);
        for layer in self
            .frozen
            .iter()
            .map(Arc::as_ref)
            .chain(std::iter::once(&self.active))
        {
            for (bits, row_id) in layer.entries() {
                graph.insert(bits, row_id);
            }
        }
        bincode::serialize(&AnnCheckpoint {
            dim: self.dim,
            bytes_per_vec: self.bytes_per_vec,
            graph,
            ef_search: self.ef_search,
        })
        .expect("ann index serializable")
    }

    /// Rehydrate from bytes produced by [`AnnIndex::freeze`].
    pub fn thaw(bytes: &[u8]) -> std::result::Result<Self, bincode::Error> {
        let checkpoint: AnnCheckpoint = bincode::deserialize(bytes)?;
        Ok(Self::from_checkpoint(checkpoint))
    }

    pub(crate) fn thaw_bounded(
        bytes: &[u8],
        limit: u64,
    ) -> std::result::Result<Self, bincode::Error> {
        let checkpoint: AnnCheckpoint = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .with_limit(limit)
            .deserialize(bytes)?;
        Ok(Self::from_checkpoint(checkpoint))
    }

    fn from_checkpoint(checkpoint: AnnCheckpoint) -> Self {
        let (m, ef_construction) = checkpoint.graph.options();
        Self {
            dim: checkpoint.dim,
            bytes_per_vec: checkpoint.bytes_per_vec,
            m,
            ef_construction,
            ef_search: checkpoint.ef_search,
            frozen: Arc::new(vec![Arc::new(checkpoint.graph)]),
            active: Hnsw::new(checkpoint.bytes_per_vec, m, ef_construction),
        }
    }

    pub(crate) fn seal(&mut self) {
        if self.active.is_empty() {
            return;
        }
        let active = std::mem::replace(
            &mut self.active,
            Hnsw::new(self.bytes_per_vec, self.m, self.ef_construction),
        );
        Arc::make_mut(&mut self.frozen).push(Arc::new(active));
        if self.frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
            self.consolidate();
        }
    }

    /// Compaction step (S1C-003): merge every frozen delta into a single new
    /// immutable base graph. The active delta is left untouched. Recall is
    /// preserved because search always merges per-layer candidates with an
    /// exact Hamming rerank, and the merged base is rebuilt from the same
    /// quantized vectors.
    pub fn merge_deltas_into_base(&mut self) {
        self.consolidate();
    }

    fn consolidate(&mut self) {
        let mut graph = Hnsw::new(self.bytes_per_vec, self.m, self.ef_construction);
        for layer in self.frozen.iter() {
            for (bits, row_id) in layer.entries() {
                graph.insert(bits, row_id);
            }
        }
        self.frozen = Arc::new(vec![Arc::new(graph)]);
    }

    /// Number of immutable layers (base + frozen deltas). `0` when nothing
    /// has been sealed yet; `1` after [`Self::merge_deltas_into_base`].
    pub fn frozen_layer_count(&self) -> usize {
        self.frozen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_search_breadth_survives_checkpoint() {
        let index = AnnIndex::with_options(8, 8, 32, 17);
        assert_eq!(index.ef_search(), 17);
        assert_eq!(AnnIndex::thaw(&index.freeze()).unwrap().ef_search(), 17);
    }

    #[test]
    fn nearest_finds_similar_vector() {
        let mut idx = AnnIndex::new(16);
        idx.insert(
            &[
                1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0,
            ],
            RowId(0),
        )
        .unwrap();
        idx.insert(
            &[
                -1.0, 1.0, -1.0, -1.0, 1.0, -1.0, -1.0, 1.0, -1.0, -1.0, 1.0, -1.0, -1.0, 1.0,
                -1.0, -1.0,
            ],
            RowId(1),
        )
        .unwrap();
        let query = [
            1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0,
        ];
        let top = idx.search(&query, 1).unwrap();
        assert_eq!(top[0].0, RowId(0));
        assert_eq!(top[0].1, 0); // identical → distance 0
    }

    #[test]
    fn quantize_uses_sign_bit() {
        let idx = AnnIndex::new(16);
        let bits = idx
            .quantize(&[
                1.0, -1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            ])
            .unwrap();
        assert_eq!(bits[0] & 0b0000_1001, 0b0000_1001); // bits 0 and 3 set
    }

    #[test]
    fn sealed_generations_merge_graphs_and_consolidate() {
        let mut writer = AnnIndex::new(8);
        for id in 0..crate::MAX_READ_GENERATION_LAYERS as u64 + 2 {
            let vector = if id % 2 == 0 { [1.0; 8] } else { [-1.0; 8] };
            writer.insert(&vector, RowId(id)).unwrap();
            writer.seal();
        }
        assert!(writer.frozen_layer_count() < crate::MAX_READ_GENERATION_LAYERS);
        let generation = writer.clone();
        writer.insert(&[1.0; 8], RowId(99)).unwrap();
        assert!(!generation
            .search(&[1.0; 8], generation.len())
            .unwrap()
            .iter()
            .any(|(row_id, _)| *row_id == RowId(99)));
        assert!(writer
            .search(&[1.0; 8], writer.len())
            .unwrap()
            .iter()
            .any(|(row_id, _)| *row_id == RowId(99)));
    }

    /// One-hot cluster vectors over `dim` dims: cluster `cluster` (of
    /// `clusters`) has a positive block of `dim / clusters` dims, negative
    /// elsewhere. All members of a cluster quantize identically, so exact
    /// top-`k` for a cluster prototype is its member set — deterministic
    /// ground truth for recall assertions.
    fn cluster_vector(dim: usize, clusters: usize, cluster: usize) -> Vec<f32> {
        let block = dim / clusters;
        (0..dim)
            .map(|d| if d / block == cluster { 1.0 } else { -1.0 })
            .collect()
    }

    fn hamming(a: &[u8], b: &[u8]) -> u32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x ^ y).count_ones())
            .sum()
    }

    #[test]
    fn base_plus_delta_recall_matches_base_only() {
        const DIM: usize = 64;
        const CLUSTERS: usize = 8;
        const MEMBERS: usize = 8;
        // Same data in two layouts: one consolidated base vs. frozen deltas
        // (each batch small enough that its layer graph is complete — 16
        // nodes under the 2*M layer-0 adjacency cap — so per-layer beams are
        // exhaustive and base+delta search is exact).
        let mut base_only = AnnIndex::new(DIM);
        let mut layered = AnnIndex::new(DIM);
        let mut data: Vec<(Vec<u8>, RowId)> = Vec::new();
        for cluster in 0..CLUSTERS {
            for member in 0..MEMBERS {
                let row_id = RowId((cluster * MEMBERS + member) as u64);
                let vector = cluster_vector(DIM, CLUSTERS, cluster);
                let bits = base_only.quantize(&vector).unwrap();
                data.push((bits, row_id));
                base_only.insert(&vector, row_id).unwrap();
                layered.insert(&vector, row_id).unwrap();
                if data.len().is_multiple_of(2 * MEMBERS) {
                    layered.seal();
                }
            }
        }
        base_only.seal();
        base_only.merge_deltas_into_base();
        assert_eq!(base_only.frozen_layer_count(), 1);
        assert_eq!(layered.frozen_layer_count(), 4);

        // The merge rebuilds the base by replaying every frozen layer's
        // entries in insertion order — the same quantized vectors in the
        // same order that built the base-only graph, with the same options
        // and deterministic HNSW seed — so the merged base IS the base-only
        // graph and recall matches exactly.
        let mut merged = layered.clone();
        merged.merge_deltas_into_base();
        assert_eq!(merged.frozen_layer_count(), 1);

        let brute_topk = |query: &[u8], k: usize| -> std::collections::HashSet<u64> {
            let mut scored: Vec<(u32, u64)> = data
                .iter()
                .map(|(bits, row_id)| (hamming(query, bits), row_id.0))
                .collect();
            scored.sort_by_key(|(distance, row_id)| (*distance, *row_id));
            scored
                .into_iter()
                .take(k)
                .map(|(_, row_id)| row_id)
                .collect()
        };

        for cluster in 0..CLUSTERS {
            let query = cluster_vector(DIM, CLUSTERS, cluster);
            let truth = brute_topk(&base_only.quantize(&query).unwrap(), MEMBERS);

            // Base+delta search: exhaustive per-layer beams make the merged
            // candidate list exact against brute force.
            let layered_results = layered.search(&query, MEMBERS).unwrap();
            let layered_hits: std::collections::HashSet<u64> =
                layered_results.iter().map(|(row_id, _)| row_id.0).collect();
            let layered_recall = truth.intersection(&layered_hits).count() as f64 / MEMBERS as f64;
            assert_eq!(
                layered_recall, 1.0,
                "base+delta search must be exact for cluster {cluster}"
            );

            // The merged base matches the base-only base result-for-result.
            let base_results = base_only.search(&query, MEMBERS).unwrap();
            let merged_results = merged.search(&query, MEMBERS).unwrap();
            assert_eq!(
                merged_results, base_results,
                "merged base must match base-only results for cluster {cluster}"
            );

            // Exact rerank: shared rows report identical Hamming distances.
            let base_distances: std::collections::HashMap<u64, u32> = base_results
                .into_iter()
                .map(|(row_id, distance)| (row_id.0, distance))
                .collect();
            for (row_id, distance) in &layered_results {
                if let Some(base_distance) = base_distances.get(&row_id.0) {
                    assert_eq!(distance, base_distance);
                }
            }
        }
    }

    #[test]
    fn merge_deltas_into_base_collapses_layers_and_preserves_results() {
        const DIM: usize = 64;
        const CLUSTERS: usize = 3;
        const MEMBERS: usize = 4;
        let mut index = AnnIndex::new(DIM);
        for cluster in 0..CLUSTERS {
            for member in 0..MEMBERS {
                let row_id = RowId((cluster * MEMBERS + member) as u64);
                index
                    .insert(&cluster_vector(DIM, CLUSTERS, cluster), row_id)
                    .unwrap();
            }
            // Last batch stays in the active delta; earlier batches freeze.
            if cluster + 1 < CLUSTERS {
                index.seal();
            }
        }
        assert_eq!(index.frozen_layer_count(), CLUSTERS - 1);

        let before: Vec<Vec<(RowId, u32)>> = (0..CLUSTERS)
            .map(|cluster| {
                index
                    .search(&cluster_vector(DIM, CLUSTERS, cluster), MEMBERS)
                    .unwrap()
            })
            .collect();

        index.merge_deltas_into_base();
        assert_eq!(index.frozen_layer_count(), 1);

        for (cluster, expected) in before.into_iter().enumerate() {
            let after = index
                .search(&cluster_vector(DIM, CLUSTERS, cluster), MEMBERS)
                .unwrap();
            assert_eq!(
                after, expected,
                "merging deltas into the base must not change cluster {cluster} results"
            );
        }
    }

    #[test]
    fn search_filtered_applies_tombstones_across_deltas() {
        const DIM: usize = 64;
        // One cluster spread over a frozen delta, the base, and the active
        // delta; every row quantizes identically (distance 0).
        let prototype = cluster_vector(DIM, 2, 0);
        let mut index = AnnIndex::new(DIM);
        for id in 0..20u64 {
            index.insert(&prototype, RowId(id)).unwrap();
            if id == 7 || id == 15 {
                index.seal();
            }
        }
        assert_eq!(index.frozen_layer_count(), 2);
        assert_eq!(index.search(&prototype, 20).unwrap().len(), 20);

        // Rows 0..8 (the whole base layer) plus 8..12 are "tombstoned".
        let tombstoned: std::collections::HashSet<u64> = (0..12).collect();
        let visible = |row_id: RowId| !tombstoned.contains(&row_id.0);
        let results = index.search_filtered(&prototype, 4, &visible).unwrap();
        assert_eq!(results.len(), 4);
        for (row_id, distance) in &results {
            assert!(visible(*row_id), "tombstoned {row_id:?} must be filtered");
            assert_eq!(*distance, 0, "exact rerank distance survives filtering");
        }
        // The filter backfills from deeper candidates rather than truncating.
        let unfiltered: std::collections::HashSet<u64> = index
            .search(&prototype, 4)
            .unwrap()
            .into_iter()
            .map(|(row_id, _)| row_id.0)
            .collect();
        assert!(unfiltered.iter().any(|row_id| tombstoned.contains(row_id)));
    }
}
