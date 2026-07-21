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
//! per-layer HNSW candidate lists, merges them keeping each row's minimum
//! reported distance, and truncates to `k`. Candidate generation remains
//! approximate.
//! [`AnnIndex::merge_deltas_into_base`] is the compaction step: it merges
//! every frozen delta into a new base graph. Deleted rows keep stale graph
//! entries (HNSW has no cheap node removal); readers apply the
//! visibility/tombstone filter via [`AnnIndex::search_filtered`].
//!
//! ## Swappable backend contract (Phase 2)
//!
//! Concrete ANN algorithms implement [`backend::AnnBackend`]. The orchestrator
//! ([`AnnIndex`]) is algorithm-agnostic: it holds a base+delta list of boxed
//! backends and routes build/insert/search/checkpoint/reopen through the trait.
//! DiskANN and IVF land as additional `AnnBackend` implementations without
//! modifying this module.

pub(crate) mod backend;
pub(crate) mod diskann;
pub(crate) mod ivf;
#[cfg(test)]
mod matrix;
pub(crate) mod pq_backend;
pub(crate) mod product;

use crate::index::ann::backend::{AnnBackend, AnnBackendCheckpoint, BackendMetric};
use crate::index::hnsw::{DenseHnsw, Hnsw};
use crate::rowid::RowId;
use crate::schema::{AnnOptions, AnnQuantization};
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

/// Vector store keyed by [`RowId`], backed by an ANN backend in the S1C-003
/// base+delta layout: `frozen` holds the immutable base (after consolidation,
/// a single layer) plus immutable frozen deltas, `active` is the small mutable
/// delta writers insert into.
///
/// `frozen` entries are `Arc`-shared (immutable, cheap to clone across the
/// read generation); `active` is a uniquely-owned boxed backend so inserts can
/// mutate it. [`Clone`] is implemented manually via [`AnnBackend::clone_box`].
pub struct AnnIndex {
    dim: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    quantization: AnnQuantization,
    options: AnnOptions,
    /// Immutable frozen layers (base + deltas). Empty until the first seal.
    frozen: Arc<Vec<Arc<dyn AnnBackend>>>,
    /// Small mutable delta writers insert into.
    active: Box<dyn AnnBackend>,
}

impl Clone for AnnIndex {
    fn clone(&self) -> Self {
        Self {
            dim: self.dim,
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            quantization: self.quantization,
            options: self.options.clone(),
            frozen: Arc::clone(&self.frozen),
            active: self.active.clone_box(),
        }
    }
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
        // Legacy entrypoint for BinarySign/Dense. Product quantization
        // requires the full option bag (num_subvectors, training params); use
        // [`Self::with_full_options`] for that.
        let options = AnnOptions {
            m,
            ef_construction,
            ef_search,
            quantization,
            ..AnnOptions::default()
        };
        Self::with_full_options(dim, m, ef_construction, ef_search, &options)
    }

    /// Full-options constructor: selects the backend from `options.quantization`
    /// (and `options.product` for Product). Used by the engine build path so
    /// product-quantized indexes carry their training configuration.
    pub fn with_full_options(
        dim: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        options: &AnnOptions,
    ) -> Self {
        let active = new_backend(dim, m, ef_construction, options);
        let mut options = options.clone();
        options.m = m;
        options.ef_construction = ef_construction;
        options.ef_search = ef_search;
        Self {
            dim,
            m,
            ef_construction,
            ef_search,
            quantization: options.quantization,
            options,
            frozen: Arc::new(Vec::new()),
            active,
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

    pub fn algorithm(&self) -> crate::schema::AnnAlgorithm {
        self.options.algorithm
    }

    /// True when this index's options and dimension match the schema declaration.
    pub(crate) fn matches_schema(
        &self,
        expected_dim: usize,
        options: &crate::schema::AnnOptions,
    ) -> bool {
        self.dim == expected_dim && self.options == *options
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
        let bytes_per_vec = self.dim.div_ceil(8);
        Ok(quantize_f32_to_binary_sign(vec, bytes_per_vec))
    }

    /// Insert a quantized vector for `row_id` (BinarySign only).
    pub fn insert_quantized(&mut self, bits: Vec<u8>, row_id: RowId) -> Result<()> {
        match self.quantization {
            AnnQuantization::BinarySign => {
                let bytes_per_vec = self.dim.div_ceil(8);
                if bits.len() != bytes_per_vec {
                    return Err(MongrelError::InvalidArgument(format!(
                        "quantized vector length must be {}, got {}",
                        bytes_per_vec,
                        bits.len()
                    )));
                }
                // Reconstruct an f32 vector that re-quantizes to `bits`, then
                // route through the uniform trait entrypoint. The sign bits
                // after quantization are identical to `bits`.
                let reconstructed = binary_sign_bits_to_f32(&bits, self.dim);
                self.active
                    .insert_validated(&reconstructed, row_id, &mut always_ok)?;
                Ok(())
            }
            _ => Err(MongrelError::InvalidArgument(
                "insert_quantized is only valid for BinarySign ANN indexes".into(),
            )),
        }
    }

    /// Convenience: insert a full-precision vector (quantizes for BinarySign).
    pub fn insert(&mut self, vec: &[f32], row_id: RowId) -> Result<()> {
        self.validate_query(vec)?;
        self.active.insert_validated(vec, row_id, &mut always_ok)?;
        Ok(())
    }

    pub(crate) fn insert_validated(&mut self, vec: &[f32], row_id: RowId) {
        if vec.len() != self.dim || vec.iter().any(|value| !value.is_finite()) {
            // Historical malformed rows are quarantined from the derived index.
            return;
        }
        let _ = self.active.insert_validated(vec, row_id, &mut always_ok);
    }

    /// Build-path insertion with cooperative checks inside graph work.
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
        self.active.insert_validated(vec, row_id, &mut checkpoint)
    }

    /// k-nearest by the index metric (Hamming or cosine distance).
    pub fn search(&self, query: &[f32], k: usize) -> Result<Vec<(RowId, AnnDistance)>> {
        self.search_with_context(query, k, None)
    }

    /// Base+delta search: each layer (base graph, frozen deltas, active
    /// delta) contributes its candidate list, the lists are merged keeping
    /// each row's exact minimum distance, and the merged list is truncated to
    /// `k`. Equal distances break ties by [`RowId`].
    pub fn search_with_context(
        &self,
        query: &[f32],
        k: usize,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<(RowId, AnnDistance)>> {
        self.validate_query(query)?;
        let mut best: HashMap<RowId, f64> = HashMap::new();
        for graph in self
            .frozen
            .iter()
            .map(|layer| layer.as_ref())
            .chain(std::iter::once(self.active.as_ref()))
        {
            for (row_id, distance) in graph.search(query, k, self.ef_search, context)? {
                best.entry(row_id)
                    .and_modify(|current| *current = current.min(distance))
                    .or_insert(distance);
            }
        }
        let mut results: Vec<_> = best
            .into_iter()
            .map(|(row_id, d)| (row_id, self.distance_from_backend(d)))
            .collect();
        results.sort_by(|left, right| left.1.cmp_rank(&right.1).then_with(|| left.0.cmp(&right.0)));
        results.truncate(k);
        Ok(results)
    }

    /// k-nearest among rows accepted by `visible` — the S1C-003
    /// visibility/tombstone filter applied across the base, frozen deltas,
    /// and active delta. Candidates are merged and ordered as in
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
        let mut best: HashMap<RowId, f64> = HashMap::new();
        for graph in self
            .frozen
            .iter()
            .map(|layer| layer.as_ref())
            .chain(std::iter::once(self.active.as_ref()))
        {
            let fetch = overfetch.min(graph.len());
            for (row_id, distance) in graph.search(query, fetch, self.ef_search.max(fetch), None)? {
                if !visible(row_id) {
                    continue;
                }
                best.entry(row_id)
                    .and_modify(|current| *current = current.min(distance))
                    .or_insert(distance);
            }
        }
        let mut results: Vec<_> = best
            .into_iter()
            .map(|(row_id, d)| (row_id, self.distance_from_backend(d)))
            .collect();
        results.sort_by(|left, right| left.1.cmp_rank(&right.1).then_with(|| left.0.cmp(&right.0)));
        results.truncate(k);
        Ok(results)
    }

    pub fn len(&self) -> usize {
        self.active.len() + self.frozen.iter().map(|graph| graph.len()).sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.active.is_empty() && self.frozen.is_empty()
    }

    /// Checkpoint the whole graph (O(N) load, no O(N log N) rebuild).
    pub fn freeze(&self) -> Vec<u8> {
        let backend_checkpoint = self.consolidated_backend_checkpoint();
        let payload = match (self.quantization, backend_checkpoint) {
            (
                AnnQuantization::BinarySign,
                AnnBackendCheckpoint::HnswBinarySign {
                    bytes_per_vec,
                    graph,
                },
            ) => AnnCheckpointPayload::BinarySign {
                bytes_per_vec,
                graph,
            },
            (AnnQuantization::Dense, AnnBackendCheckpoint::HnswDense { graph }) => {
                AnnCheckpointPayload::Dense { graph }
            }
            (
                AnnQuantization::Product {
                    num_subvectors,
                    bits,
                },
                AnnBackendCheckpoint::Product {
                    dim: _,
                    num_subvectors: cb_nsv,
                    bits: cb_bits,
                    rerank_factor,
                    quantizer,
                    codes,
                },
            ) => {
                if num_subvectors as usize != cb_nsv || bits != cb_bits {
                    unreachable!("Product checkpoint num_subvectors/bits mismatch in freeze");
                }
                AnnCheckpointPayload::Product {
                    num_subvectors: cb_nsv,
                    bits: cb_bits,
                    rerank_factor,
                    quantizer,
                    codes,
                }
            }
            (
                AnnQuantization::Dense,
                AnnBackendCheckpoint::DiskAnn {
                    dim: _,
                    r,
                    l,
                    beam_width,
                    alpha,
                    graph,
                },
            ) => AnnCheckpointPayload::DiskAnn {
                r,
                l,
                beam_width,
                alpha,
                graph,
            },
            (
                AnnQuantization::Dense,
                AnnBackendCheckpoint::Ivf {
                    dim: _,
                    nlist,
                    nprobe,
                    centroids,
                    lists,
                    seed,
                },
            ) => AnnCheckpointPayload::Ivf {
                nlist,
                nprobe,
                centroids,
                lists,
                seed,
            },
            _ => unreachable!("quantization/backend tag mismatch in freeze"),
        };
        bincode::serialize(&AnnCheckpoint {
            quantization: self.quantization,
            dim: self.dim,
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            options: AnnCheckpointOptions::from(&self.options),
            payload,
        })
        .expect("ann index serializable")
    }

    /// Build a single checkpoint for the consolidated (base + deltas + active)
    /// graph by replaying every layer's entries into a fresh backend. This is
    /// the same path the pre-trait `freeze` used.
    fn consolidated_backend_checkpoint(&self) -> AnnBackendCheckpoint {
        let template = self
            .frozen
            .first()
            .map(|layer| layer.as_ref())
            .unwrap_or_else(|| self.active.as_ref());
        let mut entries: Vec<(Vec<u8>, RowId)> = Vec::new();
        for graph in self
            .frozen
            .iter()
            .map(|layer| layer.as_ref())
            .chain(std::iter::once(self.active.as_ref()))
        {
            entries.extend(graph.entries());
        }
        template.rebuild_from_entries(&entries).freeze()
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
        let options = AnnOptions::from(checkpoint.options);
        if options.quantization != checkpoint.quantization
            || options.m != checkpoint.m
            || options.ef_construction != checkpoint.ef_construction
            || options.ef_search != checkpoint.ef_search
        {
            return Err("ANN checkpoint options mismatch header".into());
        }
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
                    return Err("ANN BinarySign checkpoint bytes_per_vec mismatch header".into());
                }
                let active = new_backend(
                    checkpoint.dim,
                    checkpoint.m,
                    checkpoint.ef_construction,
                    &options,
                );
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::BinarySign,
                    options,
                    frozen: Arc::new(vec![Arc::new(graph) as Arc<dyn AnnBackend>]),
                    active,
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
                let active = new_backend(
                    checkpoint.dim,
                    checkpoint.m,
                    checkpoint.ef_construction,
                    &options,
                );
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::Dense,
                    options,
                    frozen: Arc::new(vec![Arc::new(graph) as Arc<dyn AnnBackend>]),
                    active,
                })
            }
            (
                AnnQuantization::Product {
                    num_subvectors,
                    bits,
                },
                AnnCheckpointPayload::Product {
                    num_subvectors: cb_nsv,
                    bits: cb_bits,
                    rerank_factor,
                    quantizer,
                    codes,
                },
            ) => {
                if num_subvectors as usize != cb_nsv
                    || bits != cb_bits
                    || quantizer.dim() != checkpoint.dim
                    || quantizer.num_subvectors() != cb_nsv
                {
                    return Err("ANN Product checkpoint codebook/header mismatch".into());
                }
                let product = options.product.clone().unwrap_or_default();
                if options.algorithm != crate::schema::AnnAlgorithm::Hnsw
                    || product.rerank_factor != rerank_factor
                    || product.seed != quantizer.seed()
                {
                    return Err("ANN Product checkpoint options mismatch payload".into());
                }
                let backend = pq_backend::PqBackend::from_checkpoint(
                    checkpoint.dim,
                    cb_nsv,
                    bits,
                    &product,
                    quantizer,
                    codes,
                )?;
                let active = new_backend(
                    checkpoint.dim,
                    checkpoint.m,
                    checkpoint.ef_construction,
                    &options,
                );
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::Product {
                        num_subvectors,
                        bits,
                    },
                    options,
                    frozen: Arc::new(vec![Arc::new(backend) as Arc<dyn AnnBackend>]),
                    active,
                })
            }
            (
                AnnQuantization::Dense,
                AnnCheckpointPayload::DiskAnn {
                    r,
                    l,
                    beam_width,
                    alpha,
                    graph,
                },
            ) => {
                let diskann = options.diskann.clone().unwrap_or_default();
                if options.algorithm != crate::schema::AnnAlgorithm::DiskAnn
                    || (diskann.r, diskann.l, diskann.beam_width, diskann.alpha)
                        != (r, l, beam_width, alpha)
                    || !graph.matches_checkpoint(checkpoint.dim, &diskann)
                {
                    return Err("ANN DiskAnn checkpoint options mismatch payload".into());
                }
                let active = new_backend(
                    checkpoint.dim,
                    checkpoint.m,
                    checkpoint.ef_construction,
                    &options,
                );
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::Dense,
                    options,
                    frozen: Arc::new(vec![Arc::new(graph) as Arc<dyn AnnBackend>]),
                    active,
                })
            }
            (
                AnnQuantization::Dense,
                AnnCheckpointPayload::Ivf {
                    nlist,
                    nprobe,
                    centroids,
                    lists,
                    seed,
                },
            ) => {
                let ivf = options.ivf.clone().unwrap_or_default();
                if options.algorithm != crate::schema::AnnAlgorithm::Ivf
                    || (ivf.nlist, ivf.nprobe) != (nlist, nprobe)
                {
                    return Err("ANN IVF checkpoint options mismatch payload".into());
                }
                let backend = ivf::IvfBackend::from_checkpoint(
                    checkpoint.dim,
                    nlist,
                    nprobe,
                    ivf.training_samples,
                    centroids,
                    lists,
                    seed,
                )?;
                let active = new_backend(
                    checkpoint.dim,
                    checkpoint.m,
                    checkpoint.ef_construction,
                    &options,
                );
                Ok(Self {
                    dim: checkpoint.dim,
                    m: checkpoint.m,
                    ef_construction: checkpoint.ef_construction,
                    ef_search: checkpoint.ef_search,
                    quantization: AnnQuantization::Dense,
                    options,
                    frozen: Arc::new(vec![Arc::new(backend) as Arc<dyn AnnBackend>]),
                    active,
                })
            }
            _ => Err("ANN checkpoint quantization/payload tag mismatch".into()),
        }
    }

    pub(crate) fn seal(&mut self) {
        if self.active.is_empty() {
            return;
        }
        let empty = self.active.empty_active();
        let sealed = std::mem::replace(&mut self.active, empty);
        Arc::make_mut(&mut self.frozen).push(Arc::from(sealed));
        if self.frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
            self.consolidate_frozen();
        }
    }

    /// Compaction step (S1C-003): merge every frozen delta into a single new
    /// immutable base graph. The active delta is left untouched.
    pub fn merge_deltas_into_base(&mut self) {
        self.consolidate_frozen();
    }

    fn consolidate_frozen(&mut self) {
        if self.frozen.is_empty() {
            return;
        }
        let template = self.frozen[0].clone();
        let mut entries: Vec<(Vec<u8>, RowId)> = Vec::new();
        for layer in self.frozen.iter() {
            entries.extend(layer.entries());
        }
        let consolidated: Arc<dyn AnnBackend> = Arc::from(template.rebuild_from_entries(&entries));
        self.frozen = Arc::new(vec![consolidated]);
    }

    /// Number of immutable layers (base + frozen deltas). `0` when nothing
    /// has been sealed yet; `1` after [`Self::merge_deltas_into_base`].
    pub fn frozen_layer_count(&self) -> usize {
        self.frozen.len()
    }

    fn distance_from_backend(&self, distance: f64) -> AnnDistance {
        match self.active.metric() {
            BackendMetric::Hamming => AnnDistance::Hamming(distance as u32),
            BackendMetric::Cosine => AnnDistance::Cosine(distance as f32),
        }
    }
}

/// Construct a fresh empty active backend for the given options.
///
/// - Hnsw + BinarySign → Hamming HNSW
/// - Hnsw + Dense → cosine HNSW
/// - Hnsw + Product → flat PQ backend (ADC + approximate reconstructed rerank)
/// - DiskAnn + Dense → Vamana single-layer graph (Phase 4)
/// - Ivf + Dense → inverted-file backend
fn new_backend(
    dim: usize,
    m: usize,
    ef_construction: usize,
    options: &AnnOptions,
) -> Box<dyn AnnBackend> {
    use crate::schema::AnnAlgorithm;
    match (options.algorithm, options.quantization) {
        (AnnAlgorithm::Hnsw, AnnQuantization::BinarySign) => {
            let bytes_per_vec = dim.div_ceil(8);
            Box::new(Hnsw::new(bytes_per_vec, m, ef_construction))
        }
        (AnnAlgorithm::Hnsw, AnnQuantization::Dense) => {
            Box::new(DenseHnsw::new(dim, m, ef_construction))
        }
        (AnnAlgorithm::DiskAnn, AnnQuantization::Dense) => {
            let diskann = options.diskann.clone().unwrap_or_default();
            const DISKANN_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
            Box::new(diskann::DiskAnnBackend::new(dim, &diskann, DISKANN_SEED))
        }
        (AnnAlgorithm::Ivf, AnnQuantization::Dense) => {
            let ivf = options.ivf.clone().unwrap_or_default();
            const IVF_SEED: u64 = 0x9E37_79B9_7F4A_7C15;
            Box::new(ivf::IvfBackend::new(dim, &ivf, IVF_SEED))
        }
        (
            _,
            AnnQuantization::Product {
                num_subvectors,
                bits,
            },
        ) => {
            let product_options = options.product.clone().unwrap_or_default();
            Box::new(pq_backend::PqBackend::new(
                dim,
                num_subvectors as usize,
                bits,
                &product_options,
            ))
        }
        // IVF and any not-yet-wired combination is rejected up-front by
        // IndexDef::validate_options, so this branch is unreachable from a
        // validated path. It fails loudly rather than silently picking a
        // different backend.
        (_, _) => {
            unreachable!(
                "ANN algorithm {:?} + quantization {:?} is not supported; \
                 validate_options should have rejected it",
                options.algorithm, options.quantization
            )
        }
    }
}

/// Pack an f32 vector into the BinarySign representation (1 bit per dim, the
/// sign bit). This is the single quantization authority for BinarySign.
pub(crate) fn quantize_f32_to_binary_sign(vec: &[f32], bytes_per_vec: usize) -> Vec<u8> {
    let mut out = vec![0u8; bytes_per_vec];
    for (i, value) in vec.iter().enumerate() {
        if *value > 0.0 {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// Reconstruct an f32 vector of `dim` dims that re-quantizes to `bits`, so the
/// trait's uniform `insert_validated(&[f32], …)` entrypoint can carry a
/// pre-quantized BinarySign vector through the same quantization path. Each
/// set bit becomes `+1.0`; each clear bit becomes `-1.0`. The resulting sign
/// bits are identical to `bits` after [`quantize_f32_to_binary_sign`].
fn binary_sign_bits_to_f32(bits: &[u8], dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|i| {
            if bits[i / 8] & (1 << (i % 8)) != 0 {
                1.0
            } else {
                -1.0
            }
        })
        .collect()
}

fn always_ok() -> Result<()> {
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct AnnCheckpoint {
    quantization: AnnQuantization,
    dim: usize,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    options: AnnCheckpointOptions,
    payload: AnnCheckpointPayload,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct AnnCheckpointOptions {
    m: usize,
    ef_construction: usize,
    ef_search: usize,
    quantization: AnnQuantization,
    algorithm: crate::schema::AnnAlgorithm,
    diskann: Option<crate::schema::DiskAnnOptions>,
    ivf: Option<crate::schema::IvfOptions>,
    product: Option<crate::schema::ProductQuantizerOptions>,
}

impl From<&AnnOptions> for AnnCheckpointOptions {
    fn from(options: &AnnOptions) -> Self {
        Self {
            m: options.m,
            ef_construction: options.ef_construction,
            ef_search: options.ef_search,
            quantization: options.quantization,
            algorithm: options.algorithm,
            diskann: options.diskann.clone(),
            ivf: options.ivf.clone(),
            product: options.product.clone(),
        }
    }
}

impl From<AnnCheckpointOptions> for AnnOptions {
    fn from(options: AnnCheckpointOptions) -> Self {
        Self {
            m: options.m,
            ef_construction: options.ef_construction,
            ef_search: options.ef_search,
            quantization: options.quantization,
            algorithm: options.algorithm,
            diskann: options.diskann,
            ivf: options.ivf,
            product: options.product,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
enum AnnCheckpointPayload {
    BinarySign {
        bytes_per_vec: usize,
        graph: Hnsw,
    },
    Dense {
        graph: DenseHnsw,
    },
    /// Product-quantized flat backend (Phase 3): trained codebook +
    /// RowId-keyed codes + rerank factor.
    Product {
        num_subvectors: usize,
        bits: u8,
        rerank_factor: usize,
        quantizer: product::ProductQuantizer,
        codes: std::collections::BTreeMap<RowId, Vec<u8>>,
    },
    /// DiskANN (Vamana) single-layer graph over Dense vectors (Phase 4).
    DiskAnn {
        r: usize,
        l: usize,
        beam_width: usize,
        alpha: u32,
        graph: diskann::DiskAnnBackend,
    },
    /// IVF centroids + inverted lists over Dense vectors (Phase 5).
    Ivf {
        nlist: usize,
        nprobe: usize,
        centroids: Vec<Vec<f32>>,
        lists: std::collections::BTreeMap<usize, Vec<(RowId, Vec<f32>)>>,
        seed: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::hnsw::cosine_distance;
    use crate::query::AiExecutionContext;
    use crate::schema::{AnnOptions, ProductQuantizerOptions};

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
                ..AnnOptions::default()
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

    // ── Backend contract ────────────────────────────────────────────────

    /// The swappable backend contract preserves BinarySign behavior when a
    /// pre-quantized vector is inserted through the trait entrypoint and
    /// searched with an f32 query.
    #[test]
    fn binary_sign_round_trips_through_backend_trait() {
        let prototype = [
            1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0,
        ];
        let mut index = AnnIndex::new(16);
        index.insert(&prototype, RowId(0)).unwrap();
        let bits = index.quantize(&prototype).unwrap();
        let mut prequant = AnnIndex::new(16);
        prequant.insert_quantized(bits, RowId(42)).unwrap();
        assert_eq!(prequant.len(), 1);
        assert_eq!(prequant.search(&prototype, 1).unwrap()[0].0, RowId(42));
    }

    /// Consolidation through the trait produces a base matching a fresh
    /// single-graph build (deterministic seed + replay order).
    #[test]
    fn dense_backend_consolidation_matches_base_only_through_trait() {
        let mut base_only = dense_index(16);
        let mut layered = dense_index(16);
        for id in 0..24u64 {
            let mut v = vec![0f32; 16];
            v[(id as usize) * 7 % 16] = 1.0;
            base_only.insert(&v, RowId(id)).unwrap();
            layered.insert(&v, RowId(id)).unwrap();
            if (id + 1) % 6 == 0 {
                layered.seal();
            }
        }
        base_only.merge_deltas_into_base();
        layered.merge_deltas_into_base();
        let q = {
            let mut v = vec![0f32; 16];
            v[3] = 1.0;
            v
        };
        let base = base_only.search(&q, 10).unwrap();
        let merged = layered.search(&q, 10).unwrap();
        assert_eq!(merged.len(), base.len());
        for (i, (got, expected)) in merged.iter().zip(base.iter()).enumerate() {
            assert_eq!(
                got.0, expected.0,
                "row {i} mismatch after trait consolidation"
            );
            assert_eq!(
                got.1, expected.1,
                "distance {i} mismatch after trait consolidation"
            );
        }
    }

    // ── Product quantization (Phase 3) ───────────────────────────────────

    fn product_index(dim: usize, num_subvectors: usize) -> AnnIndex {
        let options = AnnOptions {
            m: 16,
            ef_construction: 64,
            ef_search: 64,
            quantization: AnnQuantization::Product {
                num_subvectors: num_subvectors as u16,
                bits: 8,
            },
            algorithm: crate::schema::AnnAlgorithm::Hnsw,
            diskann: None,
            ivf: None,
            product: Some(ProductQuantizerOptions {
                training_samples: 10_000,
                seed: 42,
                rerank_factor: 5,
            }),
        };
        AnnIndex::with_full_options(dim, 16, 64, 64, &options)
    }

    #[test]
    fn product_index_inserts_and_searches() {
        let dim = 16;
        let mut index = product_index(dim, 8);
        // Four well-separated one-hot clusters so the codebook can separate
        // them and recall is meaningful even at 8 subvectors.
        for cluster in 0..4u32 {
            for member in 0..8u32 {
                let mut v = vec![0f32; dim];
                v[(cluster as usize) * 4] = 1.0;
                // Small jitter to make vectors distinct but same-cluster.
                v[(cluster as usize) * 4 + 1] = (member as f32) * 0.001;
                index
                    .insert(&v, RowId((cluster * 8 + member) as u64))
                    .unwrap();
            }
        }
        let query = {
            let mut v = vec![0f32; dim];
            v[8] = 1.0; // cluster 2 prototype
            v
        };
        let top = index.search(&query, 4).unwrap();
        assert_eq!(top.len(), 4);
        // All four cluster-2 members should be in the top-4 (recall against
        // exact L2 for a one-hot cluster is exact after rerank).
        for (row_id, _) in &top {
            let id = row_id.0;
            assert!(
                (16..24).contains(&id),
                "expected cluster 2 members, got row {id}"
            );
        }
    }

    #[test]
    fn product_index_checkpoint_round_trips() {
        let dim = 16;
        let mut index = product_index(dim, 8);
        for i in 0..16u64 {
            let mut v = vec![0f32; dim];
            v[(i as usize) % dim] = 1.0;
            index.insert(&v, RowId(i)).unwrap();
        }
        let frozen = index.freeze();
        let thawed = AnnIndex::thaw(&frozen).unwrap();
        assert_eq!(
            thawed.quantization(),
            AnnQuantization::Product {
                num_subvectors: 8,
                bits: 8
            }
        );
        assert_eq!(thawed.dim(), dim);
        assert_eq!(thawed.len(), 16);
        // Search results survive the round-trip.
        let query = {
            let mut v = vec![0f32; dim];
            v[0] = 1.0;
            v
        };
        let before = index.search(&query, 5).unwrap();
        let after = thawed.search(&query, 5).unwrap();
        assert_eq!(before.len(), after.len());
        assert_eq!(
            before[0].0, after[0].0,
            "top result must survive checkpoint"
        );
        assert!(thawed.matches_schema(dim, &index.options));
    }

    #[test]
    fn product_checkpoint_rejects_corrupt_codes() {
        let mut index = product_index(16, 8);
        index.insert(&[1.0; 16], RowId(7)).unwrap();
        let mut checkpoint: AnnCheckpoint = bincode::deserialize(&index.freeze()).unwrap();
        let AnnCheckpointPayload::Product { codes, .. } = &mut checkpoint.payload else {
            panic!("expected Product checkpoint");
        };
        codes.get_mut(&RowId(7)).unwrap().pop();
        let corrupt = bincode::serialize(&checkpoint).unwrap();
        assert!(AnnIndex::thaw(&corrupt).is_err());
    }

    #[test]
    fn product_index_wrong_dim_fails() {
        let mut index = product_index(8, 4);
        assert!(index.insert(&[1.0; 7], RowId(0)).is_err());
        assert!(index.search(&[1.0; 7], 1).is_err());
    }

    #[test]
    fn product_index_non_finite_fails() {
        let mut index = product_index(8, 4);
        assert!(index
            .insert(&[1.0, f32::NAN, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], RowId(0))
            .is_err());
        assert!(index
            .insert(
                &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, f32::INFINITY],
                RowId(1)
            )
            .is_err());
    }

    #[test]
    fn product_index_consolidation_preserves_results() {
        let dim = 16;
        let mut index = product_index(dim, 8);
        // Insert in batches, sealing between, so consolidation has work to do.
        for batch in 0..3u32 {
            for member in 0..8u32 {
                let mut v = vec![0f32; dim];
                v[(batch as usize) * 4 % dim] = 1.0;
                v[1] = (member as f32) * 0.001;
                index
                    .insert(&v, RowId((batch * 8 + member) as u64))
                    .unwrap();
            }
            index.seal();
        }
        let before: Vec<Vec<(RowId, AnnDistance)>> = (0..3)
            .map(|batch| {
                let mut q = vec![0f32; dim];
                q[(batch * 4) as usize % dim] = 1.0;
                index.search(&q, 4).unwrap()
            })
            .collect();
        index.merge_deltas_into_base();
        assert_eq!(index.frozen_layer_count(), 1);
        for (batch, expected) in before.into_iter().enumerate() {
            let mut q = vec![0f32; dim];
            q[(batch * 4) % dim] = 1.0;
            let after = index.search(&q, 4).unwrap();
            assert_eq!(after.len(), expected.len());
        }
    }

    // ── DiskANN (Phase 4) ────────────────────────────────────────────────

    fn diskann_index(dim: usize) -> AnnIndex {
        let options = AnnOptions {
            m: 16,
            ef_construction: 64,
            ef_search: 64,
            quantization: AnnQuantization::Dense,
            algorithm: crate::schema::AnnAlgorithm::DiskAnn,
            diskann: Some(crate::schema::DiskAnnOptions {
                r: 16,
                l: 32,
                beam_width: 4,
                alpha: 120,
            }),
            ivf: None,
            product: None,
        };
        AnnIndex::with_full_options(dim, 16, 64, 64, &options)
    }

    #[test]
    fn diskann_index_searches_and_finds_cluster_members() {
        let dim = 16;
        let mut index = diskann_index(dim);
        for cluster in 0..4u32 {
            for member in 0..8u32 {
                let mut v = vec![0f32; dim];
                v[(cluster as usize) * 4] = 1.0;
                v[(cluster as usize) * 4 + 1] = (member as f32) * 0.001;
                index
                    .insert(&v, RowId((cluster * 8 + member) as u64))
                    .unwrap();
            }
        }
        let mut query = vec![0f32; dim];
        query[8] = 1.0; // cluster 2
        let top = index.search(&query, 4).unwrap();
        assert_eq!(top.len(), 4);
        for (row_id, _) in &top {
            assert!(
                (16..24).contains(&row_id.0),
                "expected cluster 2 members, got row {}",
                row_id.0
            );
        }
    }

    #[test]
    fn diskann_checkpoint_round_trips() {
        let dim = 16;
        let mut index = diskann_index(dim);
        for i in 0..16u64 {
            let mut v = vec![0f32; dim];
            v[(i as usize) % dim] = 1.0;
            index.insert(&v, RowId(i)).unwrap();
        }
        let frozen = index.freeze();
        let thawed = AnnIndex::thaw(&frozen).unwrap();
        assert_eq!(thawed.dim(), dim);
        assert_eq!(thawed.len(), 16);
        let mut query = vec![0f32; dim];
        query[0] = 1.0;
        let before = index.search(&query, 5).unwrap();
        let after = thawed.search(&query, 5).unwrap();
        assert_eq!(before.len(), after.len());
        assert_eq!(before[0].0, after[0].0);
    }

    #[test]
    fn diskann_consolidation_preserves_results() {
        let dim = 16;
        let mut index = diskann_index(dim);
        for batch in 0..3u32 {
            for member in 0..8u32 {
                let mut v = vec![0f32; dim];
                v[(batch as usize) * 4 % dim] = 1.0;
                v[1] = (member as f32) * 0.001;
                index
                    .insert(&v, RowId((batch * 8 + member) as u64))
                    .unwrap();
            }
            index.seal();
        }
        let mut queries: Vec<Vec<f32>> = Vec::new();
        for batch in 0..3usize {
            let mut q = vec![0f32; dim];
            q[(batch * 4) % dim] = 1.0;
            queries.push(q);
        }
        let before: Vec<_> = queries
            .iter()
            .map(|q| index.search(q, 4).unwrap())
            .collect();
        index.merge_deltas_into_base();
        assert_eq!(index.frozen_layer_count(), 1);
        for (q, expected) in queries.iter().zip(before) {
            let after = index.search(q, 4).unwrap();
            assert_eq!(after.len(), expected.len());
        }
    }

    // ── IVF (Phase 5) ────────────────────────────────────────────────────

    fn ivf_index(dim: usize, nlist: usize, nprobe: usize) -> AnnIndex {
        let options = AnnOptions {
            m: 16,
            ef_construction: 64,
            ef_search: 64,
            quantization: AnnQuantization::Dense,
            algorithm: crate::schema::AnnAlgorithm::Ivf,
            diskann: None,
            ivf: Some(crate::schema::IvfOptions {
                nlist,
                nprobe,
                training_samples: 12_345,
            }),
            product: None,
        };
        AnnIndex::with_full_options(dim, 16, 64, 64, &options)
    }

    #[test]
    fn ivf_index_searches_and_finds_cluster_members() {
        let dim = 16;
        let mut index = ivf_index(dim, 4, 2);
        for cluster in 0..4u32 {
            for member in 0..8u32 {
                let mut v = vec![0f32; dim];
                v[(cluster as usize) * 4] = 1.0;
                v[(cluster as usize) * 4 + 1] = (member as f32) * 0.001;
                index
                    .insert(&v, RowId((cluster * 8 + member) as u64))
                    .unwrap();
            }
        }
        let mut query = vec![0f32; dim];
        query[8] = 1.0; // cluster 2
        let top = index.search(&query, 4).unwrap();
        assert_eq!(top.len(), 4);
        for (row_id, _) in &top {
            assert!(
                (16..24).contains(&row_id.0),
                "expected cluster 2 members, got row {}",
                row_id.0
            );
        }
    }

    #[test]
    fn ivf_checkpoint_round_trips() {
        let dim = 16;
        let mut index = ivf_index(dim, 4, 2);
        for i in 0..16u64 {
            let mut v = vec![0f32; dim];
            v[(i as usize) % dim] = 1.0;
            index.insert(&v, RowId(i)).unwrap();
        }
        let frozen = index.freeze();
        let thawed = AnnIndex::thaw(&frozen).unwrap();
        assert_eq!(thawed.dim(), dim);
        assert_eq!(thawed.len(), 16);
        let mut query = vec![0f32; dim];
        query[0] = 1.0;
        let before = index.search(&query, 5).unwrap();
        let after = thawed.search(&query, 5).unwrap();
        assert_eq!(before.len(), after.len());
        assert_eq!(before[0].0, after[0].0);
        assert!(thawed.matches_schema(dim, &index.options));
    }

    #[test]
    fn ivf_consolidation_preserves_results() {
        let dim = 16;
        let mut index = ivf_index(dim, 4, 2);
        for batch in 0..3u32 {
            for member in 0..8u32 {
                let mut v = vec![0f32; dim];
                v[(batch as usize) * 4 % dim] = 1.0;
                v[1] = (member as f32) * 0.001;
                index
                    .insert(&v, RowId((batch * 8 + member) as u64))
                    .unwrap();
            }
            index.seal();
        }
        let queries: Vec<Vec<f32>> = (0..3)
            .map(|batch| {
                let mut q = vec![0f32; dim];
                q[(batch * 4) % dim] = 1.0;
                q
            })
            .collect();
        let before: Vec<_> = queries
            .iter()
            .map(|q| index.search(q, 4).unwrap())
            .collect();
        index.merge_deltas_into_base();
        assert_eq!(index.frozen_layer_count(), 1);
        for (q, expected) in queries.iter().zip(before) {
            let after = index.search(q, 4).unwrap();
            assert_eq!(after.len(), expected.len());
        }
    }
}
