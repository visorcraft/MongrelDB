//! ANN index — the AI-native semantic access path.
//!
//! Two representations, selected by [`AnnQuantization`]:
//!
//! - **BinarySign**: vectors are quantized to 1 bit/dim (`sign(v)`), so a
//!   768-dim embedding is 96 bytes/row and similarity is Hamming distance via
//!   SIMD `popcount(XOR)`. Backed by [`crate::index::hnsw::Hnsw`].
//! - **Dense**: full-precision `f32` vectors with cosine distance
//!   (`1 - cosine_similarity`). Backed by [`crate::index::hnsw::DenseHnsw`].
//!
//! S1C-003 base+delta layout: the index is an immutable **base** HNSW graph
//! (the single consolidated frozen layer) plus zero or more immutable frozen
//! delta graphs plus one small active mutable delta graph. Search runs
//! per-layer HNSW candidate lists, merges them keeping each row's exact
//! minimum distance (exact rerank over the stored vectors), and truncates to
//! `k` — so recall is independent of how the rows are split across layers.
//! [`AnnIndex::merge_deltas_into_base`] is the compaction step: it merges
//! every frozen delta into a new base graph. Deleted rows keep stale graph
//! entries (HNSW has no cheap node removal); readers apply the
//! visibility/tombstone filter via [`AnnIndex::search_filtered`].

use crate::index::hnsw::{DenseHnsw, Hnsw};
use crate::rowid::RowId;
use crate::schema::AnnQuantization;
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

/// Metric-aware ANN distance. Ranking is ascending for both variants (lower is
/// better). A single result set comes from one ANN index mode, so mixed
/// variants are invalid.
#[derive(Debug, Clone, Copy)]
pub enum AnnDistance {
    Hamming(u32),
    Cosine(f32),
}

impl PartialEq for AnnDistance {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Hamming(a), Self::Hamming(b)) => a == b,
            (Self::Cosine(a), Self::Cosine(b)) => a.total_cmp(b) == std::cmp::Ordering::Equal,
            _ => false,
        }
    }
}

impl Eq for AnnDistance {}

impl AnnDistance {
    fn cmp_rank(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Hamming(a), Self::Hamming(b)) => a.cmp(b),
            (Self::Cosine(a), Self::Cosine(b)) => a.total_cmp(b),
            (Self::Hamming(_), Self::Cosine(_)) => std::cmp::Ordering::Less,
            (Self::Cosine(_), Self::Hamming(_)) => std::cmp::Ordering::Greater,
        }
    }
}

/// Vector store keyed by [`RowId`], backed by HNSW graphs in the S1C-003
/// base+delta layout: `frozen` holds the immutable base (after consolidation,
/// a single layer) plus immutable frozen deltas, `active` is the small
/// mutable delta writers insert into.
#[derive(Clone)]
pub struct AnnIndex {
    dim: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    quantization: AnnQuantization,
    body: AnnBody,
}

#[derive(Clone)]
enum AnnBody {
    BinarySign {
        bytes_per_vec: usize,
        frozen: Arc<Vec<Arc<Hnsw>>>,
        active: Hnsw,
    },
    Dense {
        frozen: Arc<Vec<Arc<DenseHnsw>>>,
        active: DenseHnsw,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AnnCheckpoint {
    quantization: AnnQuantization,
    dim: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    payload: AnnCheckpointPayload,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum AnnCheckpointPayload {
    BinarySign { bytes_per_vec: usize, graph: Hnsw },
    Dense { graph: DenseHnsw },
}

impl AnnIndex {
    pub fn new(dim: usize) -> Self {
        Self::with_options(dim, M, EF_CONSTRUCTION, EF_SEARCH)
    }

    pub fn with_options(dim: usize, m: usize, ef_construction: usize, ef_search: usize) -> Self {
        Self::with_quantization(
            dim,
            m,
            ef_construction,
            ef_search,
            AnnQuantization::BinarySign,
        )
    }

    pub fn with_quantization(
        dim: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        quantization: AnnQuantization,
    ) -> Self {
        let body = match quantization {
            AnnQuantization::BinarySign => {
                let bytes_per_vec = dim.div_ceil(8);
                AnnBody::BinarySign {
                    bytes_per_vec,
                    frozen: Arc::new(Vec::new()),
                    active: Hnsw::new(bytes_per_vec, m, ef_construction),
                }
            }
            AnnQuantization::Dense => AnnBody::Dense {
                frozen: Arc::new(Vec::new()),
                active: DenseHnsw::new(dim, m, ef_construction),
            },
        };
        Self {
            dim,
            m,
            ef_construction,
            ef_search,
            quantization,
            body,
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn ef_search(&self) -> usize {
        self.ef_search
    }

    pub fn m(&self) -> usize {
        self.m
    }

    pub fn ef_construction(&self) -> usize {
        self.ef_construction
    }

    pub fn quantization(&self) -> AnnQuantization {
        self.quantization
    }

    /// True when this index's options and dimension match the schema declaration.
    pub(crate) fn matches_schema(
        &self,
        expected_dim: usize,
        options: &crate::schema::AnnOptions,
    ) -> bool {
        self.dim == expected_dim
            && self.quantization == options.quantization
            && self.m == options.m
            && self.ef_construction == options.ef_construction
            && self.ef_search == options.ef_search
    }

    fn validate_query(&self, vec: &[f32]) -> Result<()> {
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
        Ok(())
    }

    /// Quantize an f32 vector to a packed bit vector (1 bit per dim, sign bit).
    /// Only valid for [`AnnQuantization::BinarySign`].
    pub fn quantize(&self, vec: &[f32]) -> Result<Vec<u8>> {
        if !matches!(self.quantization, AnnQuantization::BinarySign) {
            return Err(MongrelError::InvalidArgument(
                "quantize is only valid for BinarySign ANN indexes".into(),
            ));
        }
        self.validate_query(vec)?;
        let bytes_per_vec = match &self.body {
            AnnBody::BinarySign { bytes_per_vec, .. } => *bytes_per_vec,
            AnnBody::Dense { .. } => unreachable!("quantization/body mismatch"),
        };
        let mut out = vec![0u8; bytes_per_vec];
        for (i, v) in vec.iter().enumerate() {
            if *v > 0.0 {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        Ok(out)
    }

    /// Insert a quantized vector for `row_id` (BinarySign only).
    pub fn insert_quantized(&mut self, bits: Vec<u8>, row_id: RowId) -> Result<()> {
        match &mut self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                active,
                ..
            } => {
                if bits.len() != *bytes_per_vec {
                    return Err(MongrelError::InvalidArgument(format!(
                        "quantized vector length must be {}, got {}",
                        bytes_per_vec,
                        bits.len()
                    )));
                }
                active.insert(bits, row_id);
                Ok(())
            }
            AnnBody::Dense { .. } => Err(MongrelError::InvalidArgument(
                "insert_quantized is only valid for BinarySign ANN indexes".into(),
            )),
        }
    }

    /// Convenience: insert a full-precision vector (quantizes for BinarySign).
    pub fn insert(&mut self, vec: &[f32], row_id: RowId) -> Result<()> {
        self.validate_query(vec)?;
        match &mut self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                active,
                ..
            } => {
                let mut bits = vec![0u8; *bytes_per_vec];
                for (i, v) in vec.iter().enumerate() {
                    if *v > 0.0 {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                active.insert(bits, row_id);
            }
            AnnBody::Dense { active, .. } => {
                active.insert(vec.to_vec(), row_id);
            }
        }
        Ok(())
    }

    pub(crate) fn insert_validated(&mut self, vec: &[f32], row_id: RowId) {
        if vec.len() != self.dim || vec.iter().any(|value| !value.is_finite()) {
            // Historical malformed rows are quarantined from the derived index.
            return;
        }
        match &mut self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                active,
                ..
            } => {
                let mut bits = vec![0u8; *bytes_per_vec];
                for (i, value) in vec.iter().enumerate() {
                    if *value > 0.0 {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                active.insert(bits, row_id);
            }
            AnnBody::Dense { active, .. } => {
                active.insert(vec.to_vec(), row_id);
            }
        }
    }

    /// Build-path insertion with cooperative checks inside Dense graph work.
    /// Malformed historical rows remain quarantined, matching
    /// [`Self::insert_validated`].
    pub(crate) fn insert_validated_with_checkpoint<F>(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        mut checkpoint: F,
    ) -> Result<()>
    where
        F: FnMut() -> Result<()>,
    {
        if vec.len() != self.dim || vec.iter().any(|value| !value.is_finite()) {
            return Ok(());
        }
        checkpoint()?;
        match &mut self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                active,
                ..
            } => {
                let mut bits = vec![0u8; *bytes_per_vec];
                for (i, value) in vec.iter().enumerate() {
                    if *value > 0.0 {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                active.insert(bits, row_id);
                Ok(())
            }
            AnnBody::Dense { active, .. } => {
                active.insert_with_checkpoint(vec.to_vec(), row_id, checkpoint)
            }
        }
    }

    /// k-nearest by the index metric (Hamming or cosine distance).
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(RowId, AnnDistance)>> {
        self.search_with_context(query, k, None)
    }

    /// Base+delta search: each layer (base graph, frozen deltas, active
    /// delta) contributes its HNSW candidate list, the lists are merged
    /// keeping each row's exact minimum distance, and the merged list is
    /// truncated to `k`. Equal distances break ties by [`RowId`].
    pub fn search_with_context(
        &self,
        query: &[f32],
        k: usize,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<(RowId, AnnDistance)>> {
        self.validate_query(query)?;
        match &self.body {
            AnnBody::BinarySign { frozen, active, .. } => {
                let q = self.quantize(query)?;
                let mut best = HashMap::<RowId, u32>::new();
                for graph in frozen
                    .iter()
                    .map(Arc::as_ref)
                    .chain(std::iter::once(active))
                {
                    for (row_id, distance) in
                        graph.search_with_context(&q, k, self.ef_search, context)?
                    {
                        best.entry(row_id)
                            .and_modify(|current| *current = (*current).min(distance))
                            .or_insert(distance);
                    }
                }
                let mut results: Vec<_> = best
                    .into_iter()
                    .map(|(row_id, d)| (row_id, AnnDistance::Hamming(d)))
                    .collect();
                results.sort_by(|left, right| {
                    left.1.cmp_rank(&right.1).then_with(|| left.0.cmp(&right.0))
                });
                results.truncate(k);
                Ok(results)
            }
            AnnBody::Dense { frozen, active } => {
                let mut best = HashMap::<RowId, f32>::new();
                for graph in frozen
                    .iter()
                    .map(Arc::as_ref)
                    .chain(std::iter::once(active))
                {
                    for (row_id, distance) in
                        graph.search_with_context(query, k, self.ef_search, context)?
                    {
                        best.entry(row_id)
                            .and_modify(|current| {
                                if distance.total_cmp(current).is_lt() {
                                    *current = distance;
                                }
                            })
                            .or_insert(distance);
                    }
                }
                let mut results: Vec<_> = best
                    .into_iter()
                    .map(|(row_id, d)| (row_id, AnnDistance::Cosine(d)))
                    .collect();
                results.sort_by(|left, right| {
                    left.1.cmp_rank(&right.1).then_with(|| left.0.cmp(&right.0))
                });
                results.truncate(k);
                Ok(results)
            }
        }
    }

    /// k-nearest among rows accepted by `visible` — the S1C-003
    /// visibility/tombstone filter applied across the base, frozen deltas,
    /// and active delta. Candidates are merged and reranked exactly as in
    /// [`Self::search_with_context`]; each layer is over-fetched (see
    /// [`FILTERED_SEARCH_OVERFETCH`]) so visible rows ranked behind filtered
    /// ones still surface.
    pub fn search_filtered(
        &self,
        query: &[f32],
        k: usize,
        visible: &dyn Fn(RowId) -> bool,
    ) -> Result<Vec<(RowId, AnnDistance)>> {
        self.validate_query(query)?;
        let overfetch = k
            .saturating_mul(FILTERED_SEARCH_OVERFETCH)
            .max(k.saturating_add(16));
        match &self.body {
            AnnBody::BinarySign { frozen, active, .. } => {
                let q = self.quantize(query)?;
                let mut best = HashMap::<RowId, u32>::new();
                for graph in frozen
                    .iter()
                    .map(Arc::as_ref)
                    .chain(std::iter::once(active))
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
                let mut results: Vec<_> = best
                    .into_iter()
                    .map(|(row_id, d)| (row_id, AnnDistance::Hamming(d)))
                    .collect();
                results.sort_by(|left, right| {
                    left.1.cmp_rank(&right.1).then_with(|| left.0.cmp(&right.0))
                });
                results.truncate(k);
                Ok(results)
            }
            AnnBody::Dense { frozen, active } => {
                let mut best = HashMap::<RowId, f32>::new();
                for graph in frozen
                    .iter()
                    .map(Arc::as_ref)
                    .chain(std::iter::once(active))
                {
                    let fetch = overfetch.min(graph.len());
                    for (row_id, distance) in graph.search(query, fetch, self.ef_search.max(fetch))
                    {
                        if !visible(row_id) {
                            continue;
                        }
                        best.entry(row_id)
                            .and_modify(|current| {
                                if distance.total_cmp(current).is_lt() {
                                    *current = distance;
                                }
                            })
                            .or_insert(distance);
                    }
                }
                let mut results: Vec<_> = best
                    .into_iter()
                    .map(|(row_id, d)| (row_id, AnnDistance::Cosine(d)))
                    .collect();
                results.sort_by(|left, right| {
                    left.1.cmp_rank(&right.1).then_with(|| left.0.cmp(&right.0))
                });
                results.truncate(k);
                Ok(results)
            }
        }
    }

    pub fn len(&self) -> usize {
        match &self.body {
            AnnBody::BinarySign { frozen, active, .. } => {
                active.len() + frozen.iter().map(|graph| graph.len()).sum::<usize>()
            }
            AnnBody::Dense { frozen, active } => {
                active.len() + frozen.iter().map(|graph| graph.len()).sum::<usize>()
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        match &self.body {
            AnnBody::BinarySign { frozen, active, .. } => active.is_empty() && frozen.is_empty(),
            AnnBody::Dense { frozen, active } => active.is_empty() && frozen.is_empty(),
        }
    }

    /// Checkpoint the whole HNSW graph (O(N) load, no O(N log N) rebuild).
    pub fn freeze(&self) -> Vec<u8> {
        let payload = match &self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                frozen,
                active,
            } => {
                let mut graph = Hnsw::new(*bytes_per_vec, self.m, self.ef_construction);
                for layer in frozen
                    .iter()
                    .map(Arc::as_ref)
                    .chain(std::iter::once(active))
                {
                    for (bits, row_id) in layer.entries() {
                        graph.insert(bits, row_id);
                    }
                }
                AnnCheckpointPayload::BinarySign {
                    bytes_per_vec: *bytes_per_vec,
                    graph,
                }
            }
            AnnBody::Dense { frozen, active } => {
                let mut graph = DenseHnsw::new(self.dim, self.m, self.ef_construction);
                for layer in frozen
                    .iter()
                    .map(Arc::as_ref)
                    .chain(std::iter::once(active))
                {
                    for (vec, row_id) in layer.entries() {
                        graph.insert(vec, row_id);
                    }
                }
                AnnCheckpointPayload::Dense { graph }
            }
        };
        bincode::serialize(&AnnCheckpoint {
            quantization: self.quantization,
            dim: self.dim,
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            payload,
        })
        .expect("ann index serializable")
    }

    /// Rehydrate from bytes produced by [`AnnIndex::freeze`].
    pub fn thaw(bytes: &[u8]) -> std::result::Result<Self, bincode::Error> {
        let checkpoint: AnnCheckpoint = bincode::deserialize(bytes)?;
        Self::from_checkpoint(checkpoint).map_err(|msg| Box::new(bincode::ErrorKind::Custom(msg)))
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
        Self::from_checkpoint(checkpoint).map_err(|msg| Box::new(bincode::ErrorKind::Custom(msg)))
    }

    fn from_checkpoint(checkpoint: AnnCheckpoint) -> std::result::Result<Self, String> {
        match (checkpoint.quantization, checkpoint.payload) {
            (
                AnnQuantization::BinarySign,
                AnnCheckpointPayload::BinarySign {
                    bytes_per_vec,
                    graph,
                },
            ) => {
                let (m, ef_construction) = graph.options();
                if m != checkpoint.m || ef_construction != checkpoint.ef_construction {
                    return Err("ANN BinarySign checkpoint graph options mismatch header".into());
                }
                if bytes_per_vec != checkpoint.dim.div_ceil(8) {
                    return Err("ANN BinarySign checkpoint bytes_per_vec mismatch".into());
                }
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::BinarySign,
                    body: AnnBody::BinarySign {
                        bytes_per_vec,
                        frozen: Arc::new(vec![Arc::new(graph)]),
                        active: Hnsw::new(bytes_per_vec, checkpoint.m, checkpoint.ef_construction),
                    },
                })
            }
            (AnnQuantization::Dense, AnnCheckpointPayload::Dense { graph }) => {
                let (m, ef_construction) = graph.options();
                if m != checkpoint.m || ef_construction != checkpoint.ef_construction {
                    return Err("ANN Dense checkpoint graph options mismatch header".into());
                }
                if graph.dim() != checkpoint.dim {
                    return Err("ANN Dense checkpoint graph dim mismatch header".into());
                }
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::Dense,
                    body: AnnBody::Dense {
                        frozen: Arc::new(vec![Arc::new(graph)]),
                        active: DenseHnsw::new(
                            checkpoint.dim,
                            checkpoint.m,
                            checkpoint.ef_construction,
                        ),
                    },
                })
            }
            _ => Err("ANN checkpoint quantization/payload tag mismatch".into()),
        }
    }

    pub(crate) fn seal(&mut self) {
        match &mut self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                frozen,
                active,
            } => {
                if active.is_empty() {
                    return;
                }
                let sealed = std::mem::replace(
                    active,
                    Hnsw::new(*bytes_per_vec, self.m, self.ef_construction),
                );
                Arc::make_mut(frozen).push(Arc::new(sealed));
                if frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
                    Self::consolidate_binary(bytes_per_vec, frozen, self.m, self.ef_construction);
                }
            }
            AnnBody::Dense { frozen, active } => {
                if active.is_empty() {
                    return;
                }
                let sealed = std::mem::replace(
                    active,
                    DenseHnsw::new(self.dim, self.m, self.ef_construction),
                );
                Arc::make_mut(frozen).push(Arc::new(sealed));
                if frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
                    Self::consolidate_dense(self.dim, frozen, self.m, self.ef_construction);
                }
            }
        }
    }

    /// Compaction step (S1C-003): merge every frozen delta into a single new
    /// immutable base graph. The active delta is left untouched.
    pub fn merge_deltas_into_base(&mut self) {
        match &mut self.body {
            AnnBody::BinarySign {
                bytes_per_vec,
                frozen,
                ..
            } => Self::consolidate_binary(bytes_per_vec, frozen, self.m, self.ef_construction),
            AnnBody::Dense { frozen, .. } => {
                Self::consolidate_dense(self.dim, frozen, self.m, self.ef_construction)
            }
        }
    }

    fn consolidate_binary(
        bytes_per_vec: &usize,
        frozen: &mut Arc<Vec<Arc<Hnsw>>>,
        m: usize,
        ef_construction: usize,
    ) {
        if frozen.is_empty() {
            return;
        }
        let mut graph = Hnsw::new(*bytes_per_vec, m, ef_construction);
        for layer in frozen.iter() {
            for (bits, row_id) in layer.entries() {
                graph.insert(bits, row_id);
            }
        }
        *frozen = Arc::new(vec![Arc::new(graph)]);
    }

    fn consolidate_dense(
        dim: usize,
        frozen: &mut Arc<Vec<Arc<DenseHnsw>>>,
        m: usize,
        ef_construction: usize,
    ) {
        if frozen.is_empty() {
            return;
        }
        let mut graph = DenseHnsw::new(dim, m, ef_construction);
        for layer in frozen.iter() {
            for (vec, row_id) in layer.entries() {
                graph.insert(vec, row_id);
            }
        }
        *frozen = Arc::new(vec![Arc::new(graph)]);
    }

    /// Number of immutable layers (base + frozen deltas). `0` when nothing
    /// has been sealed yet; `1` after [`Self::merge_deltas_into_base`].
    pub fn frozen_layer_count(&self) -> usize {
        match &self.body {
            AnnBody::BinarySign { frozen, .. } => frozen.len(),
            AnnBody::Dense { frozen, .. } => frozen.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::cosine_distance;
    use crate::query::AiExecutionContext;
    use crate::schema::AnnOptions;

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
        assert_eq!(top[0].1, AnnDistance::Hamming(0)); // identical → distance 0
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
            let base_distances: std::collections::HashMap<u64, AnnDistance> = base_results
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

        let before: Vec<Vec<(RowId, AnnDistance)>> = (0..CLUSTERS)
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
            assert_eq!(
                *distance,
                AnnDistance::Hamming(0),
                "exact rerank distance survives filtering"
            );
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

    // ── Dense ANN ────────────────────────────────────────────────────────

    fn dense_index(dim: usize) -> AnnIndex {
        AnnIndex::with_quantization(dim, 16, 64, 64, AnnQuantization::Dense)
    }

    #[test]
    fn dense_checkpoint_round_trip() {
        let mut index = AnnIndex::with_quantization(8, 8, 32, 17, AnnQuantization::Dense);
        index
            .insert(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], RowId(1))
            .unwrap();
        index
            .insert(&[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], RowId(2))
            .unwrap();
        let thawed = AnnIndex::thaw(&index.freeze()).unwrap();
        assert_eq!(thawed.quantization(), AnnQuantization::Dense);
        assert_eq!(thawed.dim(), 8);
        assert_eq!(thawed.m(), 8);
        assert_eq!(thawed.ef_construction(), 32);
        assert_eq!(thawed.ef_search(), 17);
        assert_eq!(thawed.len(), 2);
        let top = thawed
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1)
            .unwrap();
        assert_eq!(top[0].0, RowId(1));
        assert!(matches!(top[0].1, AnnDistance::Cosine(d) if d.abs() < 1e-6));
        assert!(thawed.matches_schema(
            8,
            &AnnOptions {
                m: 8,
                ef_construction: 32,
                ef_search: 17,
                quantization: AnnQuantization::Dense,
            }
        ));
    }

    #[test]
    fn dense_zero_norm_distance_is_one() {
        let mut index = dense_index(4);
        index.insert(&[0.0, 0.0, 0.0, 0.0], RowId(0)).unwrap();
        index.insert(&[1.0, 0.0, 0.0, 0.0], RowId(1)).unwrap();
        // Zero query vs any stored → distance 1.
        let hits = index.search(&[0.0, 0.0, 0.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        for (_, d) in &hits {
            assert_eq!(*d, AnnDistance::Cosine(1.0));
        }
        // Non-zero query vs zero stored → distance 1 for that row.
        let hits = index.search(&[1.0, 0.0, 0.0, 0.0], 2).unwrap();
        let zero_hit = hits.iter().find(|(r, _)| *r == RowId(0)).unwrap();
        assert_eq!(zero_hit.1, AnnDistance::Cosine(1.0));
        let unit_hit = hits.iter().find(|(r, _)| *r == RowId(1)).unwrap();
        assert!(matches!(unit_hit.1, AnnDistance::Cosine(d) if d.abs() < 1e-6));
    }

    #[test]
    fn dense_wrong_dim_nan_inf_fail() {
        let mut index = dense_index(4);
        index.insert(&[1.0, 0.0, 0.0, 0.0], RowId(0)).unwrap();
        assert!(index.search(&[1.0, 0.0, 0.0], 1).is_err());
        assert!(index.search(&[1.0, 0.0, 0.0, f32::NAN], 1).is_err());
        assert!(index.search(&[1.0, 0.0, 0.0, f32::INFINITY], 1).is_err());
        assert!(index.insert(&[1.0, f32::NAN, 0.0, 0.0], RowId(1)).is_err());
        assert!(index
            .insert(&[1.0, 0.0, 0.0, f32::NEG_INFINITY], RowId(2))
            .is_err());
        assert!(index.insert(&[1.0; 3], RowId(3)).is_err());
    }

    #[test]
    fn dense_distances_match_brute_force_cosine() {
        let mut index = dense_index(8);
        let mut seed = 99u64;
        let mut data = Vec::new();
        for i in 0..40u64 {
            let mut v = vec![0f32; 8];
            for x in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *x = ((seed >> 40) as i32 as f32) / (i32::MAX as f32);
            }
            index.insert(&v, RowId(i)).unwrap();
            data.push(v);
        }
        let query = data[7].clone();
        let mut brute: Vec<(RowId, f32)> = data
            .iter()
            .enumerate()
            .map(|(i, v)| (RowId(i as u64), cosine_distance(&query, v)))
            .collect();
        brute.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        brute.truncate(10);
        let got = index.search(&query, 10).unwrap();
        assert_eq!(got.len(), 10);
        for (i, (row_id, dist)) in got.iter().enumerate() {
            assert_eq!(*row_id, brute[i].0, "row order mismatch at {i}");
            match dist {
                AnnDistance::Cosine(d) => {
                    assert!(
                        (d - brute[i].1).abs() < 1e-5,
                        "distance mismatch at {i}: {d} vs {}",
                        brute[i].1
                    );
                }
                _ => panic!("expected cosine distance"),
            }
        }
    }

    #[test]
    fn dense_hnsw_recall_at_10() {
        let n = 300;
        let dim = 32;
        let mut index = dense_index(dim);
        let mut data: Vec<(Vec<f32>, RowId)> = Vec::with_capacity(n);
        let mut seed = 12345u64;
        for i in 0..n {
            let mut v = vec![0f32; dim];
            for b in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let u = ((seed >> 33) as u32) as f32 / (u32::MAX as f32);
                *b = u * 2.0 - 1.0;
            }
            data.push((v.clone(), RowId(i as u64)));
            index.insert(&v, RowId(i as u64)).unwrap();
        }

        let brute_topk = |q: &[f32], k: usize| -> std::collections::HashSet<u64> {
            let mut s: Vec<(f32, u64)> = data
                .iter()
                .map(|(v, rid)| (cosine_distance(q, v), rid.0))
                .collect();
            s.sort_by(|(da, ra), (db, rb)| da.total_cmp(db).then_with(|| ra.cmp(rb)));
            s.into_iter().take(k).map(|(_, r)| r).collect()
        };

        let mut total_recall = 0.0;
        let queries = 20;
        for qi in 0..queries {
            let q = data[qi * 7 % n].0.clone();
            let truth = brute_topk(&q, 10);
            let got: std::collections::HashSet<u64> = index
                .search(&q, 10)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.0)
                .collect();
            total_recall += truth.intersection(&got).count() as f64 / 10.0;
        }
        let avg = total_recall / queries as f64;
        assert!(avg >= 0.90, "dense AnnIndex recall@10 too low: {avg:.2}");
    }

    #[test]
    fn dense_max_work_stops_with_typed_error() {
        let mut index = dense_index(64);
        for i in 0..50u64 {
            let mut v = vec![0f32; 64];
            v[(i as usize) % 64] = 1.0;
            index.insert(&v, RowId(i)).unwrap();
        }
        let context = AiExecutionContext::new(None, 1);
        let err = index
            .search_with_context(&[1.0; 64], 10, Some(&context))
            .unwrap_err();
        assert!(
            matches!(err, MongrelError::WorkBudgetExceeded),
            "expected WorkBudgetExceeded, got {err:?}"
        );
    }

    #[test]
    fn dense_deadline_stops_with_typed_error() {
        let mut index = dense_index(64);
        for i in 0..80u64 {
            let mut v = vec![0f32; 64];
            v[(i as usize) % 64] = 1.0;
            index.insert(&v, RowId(i)).unwrap();
        }
        let deadline = Some(std::time::Instant::now() - std::time::Duration::from_millis(1));
        let context = AiExecutionContext::new(deadline, usize::MAX);
        let err = index
            .search_with_context(&[1.0; 64], 10, Some(&context))
            .unwrap_err();
        // Deadline surfaces as Cancelled / DeadlineExceeded depending on control.
        assert!(
            matches!(
                err,
                MongrelError::Cancelled
                    | MongrelError::DeadlineExceeded
                    | MongrelError::WorkBudgetExceeded
            ),
            "expected deadline/cancel error, got {err:?}"
        );
    }

    #[test]
    fn binary_sign_unchanged_by_dense_variant() {
        let mut bin = AnnIndex::new(8);
        bin.insert(&[1.0; 8], RowId(0)).unwrap();
        assert_eq!(bin.quantization(), AnnQuantization::BinarySign);
        assert_eq!(
            bin.search(&[1.0; 8], 1).unwrap()[0].1,
            AnnDistance::Hamming(0)
        );
    }
}
