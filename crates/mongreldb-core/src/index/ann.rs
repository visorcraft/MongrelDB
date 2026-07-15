//! Binary-quantized ANN index — the AI-native semantic access path.
//!
//! Vectors are quantized to 1 bit/dim (`sign(v)`), so a 768-dim embedding is
//! 96 bytes/row and similarity is Hamming distance via SIMD `popcount(XOR)`.
//! Search uses a real **HNSW** graph ([`crate::index::hnsw::Hnsw`]); an agent
//! composes `semsearch(text, k)` with the other row-id-space primitives.

use crate::index::hnsw::Hnsw;
use crate::rowid::RowId;
use crate::{MongrelError, Result};
use std::collections::HashMap;
use std::sync::Arc;

const M: usize = 16;
const EF_CONSTRUCTION: usize = 64;
const EF_SEARCH: usize = 64;

/// Quantized vector store keyed by [`RowId`], backed by an HNSW graph.
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
        let (m, ef_construction) = checkpoint.graph.options();
        Ok(Self {
            dim: checkpoint.dim,
            bytes_per_vec: checkpoint.bytes_per_vec,
            m,
            ef_construction,
            ef_search: checkpoint.ef_search,
            frozen: Arc::new(vec![Arc::new(checkpoint.graph)]),
            active: Hnsw::new(checkpoint.bytes_per_vec, m, ef_construction),
        })
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

    fn consolidate(&mut self) {
        let mut graph = Hnsw::new(self.bytes_per_vec, self.m, self.ef_construction);
        for layer in self.frozen.iter() {
            for (bits, row_id) in layer.entries() {
                graph.insert(bits, row_id);
            }
        }
        self.frozen = Arc::new(vec![Arc::new(graph)]);
    }

    #[cfg(test)]
    pub(crate) fn frozen_layer_count(&self) -> usize {
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
}
