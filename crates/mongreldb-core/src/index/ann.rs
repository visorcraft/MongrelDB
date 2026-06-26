//! Binary-quantized ANN index — the AI-native semantic access path.
//!
//! Vectors are quantized to 1 bit/dim (`sign(v)`), so a 768-dim embedding is
//! 96 bytes/row and similarity is Hamming distance via SIMD `popcount(XOR)`.
//! Search uses a real **HNSW** graph ([`crate::index::hnsw::Hnsw`]); an agent
//! composes `semsearch(text, k)` with the other row-id-space primitives.

use crate::index::hnsw::Hnsw;
use crate::rowid::RowId;

const M: usize = 16;
const EF_CONSTRUCTION: usize = 64;
const EF_SEARCH: usize = 64;

/// Quantized vector store keyed by [`RowId`], backed by an HNSW graph.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct AnnIndex {
    dim: usize,
    bytes_per_vec: usize,
    graph: Hnsw,
}

impl AnnIndex {
    pub fn new(dim: usize) -> Self {
        let bytes_per_vec = dim.div_ceil(8);
        Self {
            dim,
            bytes_per_vec,
            graph: Hnsw::new(bytes_per_vec, M, EF_CONSTRUCTION),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    /// Quantize an f32 vector to a packed bit vector (1 bit per dim, sign bit).
    pub fn quantize(&self, vec: &[f32]) -> Vec<u8> {
        assert_eq!(vec.len(), self.dim, "embedding dimension mismatch");
        let mut out = vec![0u8; self.bytes_per_vec];
        for (i, v) in vec.iter().enumerate() {
            if *v > 0.0 {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        out
    }

    /// Insert a quantized vector for `row_id`.
    pub fn insert_quantized(&mut self, bits: Vec<u8>, row_id: RowId) {
        assert_eq!(bits.len(), self.bytes_per_vec, "quantized length mismatch");
        self.graph.insert(bits, row_id);
    }

    /// Convenience: quantize then insert.
    pub fn insert(&mut self, vec: &[f32], row_id: RowId) {
        let bits = self.quantize(vec);
        self.insert_quantized(bits, row_id);
    }

    /// k-nearest by Hamming distance.
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(RowId, u32)> {
        let q = self.quantize(query);
        self.graph.search(&q, k, EF_SEARCH)
    }

    pub fn len(&self) -> usize {
        self.graph.len()
    }

    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    /// Checkpoint the whole HNSW graph (O(N) load, no O(N log N) rebuild).
    pub fn freeze(&self) -> Vec<u8> {
        bincode::serialize(self).expect("ann index serializable")
    }

    /// Rehydrate from bytes produced by [`AnnIndex::freeze`].
    pub fn thaw(bytes: &[u8]) -> std::result::Result<Self, bincode::Error> {
        bincode::deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_finds_similar_vector() {
        let mut idx = AnnIndex::new(16);
        idx.insert(
            &[
                1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0,
            ],
            RowId(0),
        );
        idx.insert(
            &[
                -1.0, 1.0, -1.0, -1.0, 1.0, -1.0, -1.0, 1.0, -1.0, -1.0, 1.0, -1.0, -1.0, 1.0,
                -1.0, -1.0,
            ],
            RowId(1),
        );
        let query = [
            1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0, -1.0, 1.0, 1.0,
        ];
        let top = idx.search(&query, 1);
        assert_eq!(top[0].0, RowId(0));
        assert_eq!(top[0].1, 0); // identical → distance 0
    }

    #[test]
    fn quantize_uses_sign_bit() {
        let idx = AnnIndex::new(16);
        let bits = idx.quantize(&[
            1.0, -1.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ]);
        assert_eq!(bits[0] & 0b0000_1001, 0b0000_1001); // bits 0 and 3 set
    }
}
