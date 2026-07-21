//! Swappable ANN backend contract.
//!
//! Phase 2 (Dense ANN and swappable algorithms) introduces a pluggable backend
//! layer behind [`AnnIndex`]. Today the only backends are the two HNSW
//! implementations in [`crate::index::hnsw`] (`Hnsw` for BinarySign Hamming and
//! `DenseHnsw` for Dense cosine). DiskANN and IVF will land as additional
//! [`AnnBackend`] implementations without reopening the orchestrator.
//!
//! Each backend owns:
//!
//! - its graph/structure storage and its native vector representation
//!   (BinarySign backends store packed-bit vectors; Dense backends store f32
//!   vectors),
//! - its distance metric (reported via [`AnnBackend::metric`]),
//! - cooperative cancellation/deadline checks during insert and search,
//! - checkpoint freeze/thaw (the [`AnnBackendCheckpoint`] envelope is
//!   serialized into the existing `_idx/global.idx` GCM-encrypted record),
//! - a bounded in-memory base+delta layout (the orchestrator holds an immutable
//!   frozen base plus a small mutable active delta per backend; consolidation
//!   replays `entries()`).
//!
//! The orchestrator ([`super::AnnIndex`]) is responsible for dimension,
//! finite-value and zero-norm validation, and for over-fetch + merge across
//! frozen + active layers, and for truncation/tie-breaking by `RowId`. Backends
//! only ever see already-validated f32 vectors on the insert/search path; the
//! BinarySign quantization (f32 → sign bit) lives in the backend itself so the
//! trait surface is uniform.

use crate::index::ann::product::ProductQuantizer;
use crate::index::hnsw::{DenseHnsw, Hnsw};
use crate::query::AiExecutionContext;
use crate::rowid::RowId;
use crate::Result;
use std::collections::BTreeMap;

/// The distance metric a backend ranks results by. Mirrors [`super::AnnDistance`]
/// but lives in the backend layer so a backend can report its metric before the
/// orchestrator assembles a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BackendMetric {
    /// Hamming over packed-bit vectors (BinarySign).
    Hamming,
    /// Cosine distance over full-precision f32 vectors (Dense).
    Cosine,
}

/// A single backend's checkpoint payload. One variant per concrete backend so
/// the orchestrator can dispatch thaw without knowing the backend's internals.
///
/// This is the inner payload of [`super::AnnCheckpointPayload`]; the outer
/// envelope carries the quantization tag so a quantization/payload mismatch
/// fails closed.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) enum AnnBackendCheckpoint {
    /// BinarySign HNSW graph (`bytes_per_vec` packed-bit vectors).
    HnswBinarySign { bytes_per_vec: usize, graph: Hnsw },
    /// Dense cosine HNSW graph (full-precision f32 vectors).
    HnswDense { graph: DenseHnsw },
    /// DiskANN (Vamana) single-layer graph over Dense vectors (Phase 4).
    DiskAnn {
        dim: usize,
        r: usize,
        l: usize,
        beam_width: usize,
        alpha: u32,
        graph: crate::index::ann::diskann::DiskAnnBackend,
    },
    /// Product-quantized flat backend: trained codebook + RowId-keyed codes.
    Product {
        dim: usize,
        num_subvectors: usize,
        bits: u8,
        rerank_factor: usize,
        quantizer: ProductQuantizer,
        codes: BTreeMap<RowId, Vec<u8>>,
    },
    /// IVF backend (Phase 5): trained centroids + per-cell inverted lists.
    Ivf {
        dim: usize,
        nlist: usize,
        nprobe: usize,
        centroids: Vec<Vec<f32>>,
        lists: BTreeMap<usize, Vec<(RowId, Vec<f32>)>>,
        seed: u64,
    },
}

/// The contract every concrete ANN algorithm implements. The orchestrator
/// ([`super::AnnIndex`]) holds one active instance and zero or more frozen
/// immutable instances (the S1C-003 base+delta layout).
///
/// Implementations are boxed (`Box<dyn AnnBackend>`) so new algorithms land as
/// additional variants without modifying the orchestrator dispatch. Clone is
/// supported via [`AnnBackend::clone_box`].
pub(crate) trait AnnBackend: Send + Sync {
    /// The distance metric this backend ranks by.
    fn metric(&self) -> BackendMetric;

    /// Number of indexed rows.
    fn len(&self) -> usize;

    /// Whether the backend holds no rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Insert an already-validated f32 vector. The orchestrator guarantees the
    /// slice has the configured dimension and finite values; backends may still
    /// run cooperative cancellation via `checkpoint` (returning early on cancel
    /// or deadline). BinarySign backends quantize f32 → sign bits internally.
    fn insert_validated(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()>;

    /// k-nearest candidates from this backend's graph. Returns raw
    /// `(row_id, distance)` pairs as `f64` so a single return type spans both
    /// metrics; the orchestrator converts to [`super::AnnDistance`] based on
    /// [`metric`](AnnBackend::metric). `ef` is the backend's search beam width.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, f64)>>;

    /// Iterate `(native_bytes, row_id)` for every stored vector, in insertion
    /// order. Used by consolidation (rebuild a fresh base graph from the union
    /// of frozen layers). For BinarySign backends the bytes are the packed-bit
    /// vector; for Dense backends the bytes are the little-endian f32 vector.
    /// Round-trips through [`rebuild_from_entries`](Self::rebuild_from_entries).
    fn entries(&self) -> Vec<(Vec<u8>, RowId)>;

    /// Serialize this backend's graph for checkpoint. The orchestrator wraps
    /// the returned [`AnnBackendCheckpoint`] in its quantization-tagged envelope.
    fn freeze(&self) -> AnnBackendCheckpoint;

    /// Reconstruct an empty active delta backend with the same construction
    /// parameters (used after sealing the active delta into the frozen list).
    fn empty_active(&self) -> Box<dyn AnnBackend>;

    /// Rebuild a fresh backend of the same kind by replaying `entries()` into a
    /// new instance. Used by consolidation: the merged base must be built from
    /// the same entries in the same order with the same deterministic seed, so
    /// the result matches a base-only build.
    fn rebuild_from_entries(&self, entries: &[(Vec<u8>, RowId)]) -> Box<dyn AnnBackend>;

    /// Clone into a boxed value (so `AnnIndex` can clone its active delta).
    fn clone_box(&self) -> Box<dyn AnnBackend>;
}

// ── Hnsw (BinarySign / Hamming) backend ──────────────────────────────────────

impl AnnBackend for Hnsw {
    fn metric(&self) -> BackendMetric {
        BackendMetric::Hamming
    }

    fn len(&self) -> usize {
        Hnsw::len(self)
    }

    fn insert_validated(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        checkpoint()?;
        let bits = super::quantize_f32_to_binary_sign(vec, self.bytes_per_vec());
        self.insert(bits, row_id);
        Ok(())
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, f64)>> {
        let bits = super::quantize_f32_to_binary_sign(query, self.bytes_per_vec());
        Ok(self
            .search_with_context(&bits, k, ef, context)?
            .into_iter()
            .map(|(row_id, distance)| (row_id, f64::from(distance)))
            .collect())
    }

    fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        Hnsw::entries(self).collect()
    }

    fn freeze(&self) -> AnnBackendCheckpoint {
        AnnBackendCheckpoint::HnswBinarySign {
            bytes_per_vec: self.bytes_per_vec(),
            graph: self.clone(),
        }
    }

    fn empty_active(&self) -> Box<dyn AnnBackend> {
        let (m, ef_construction) = self.options();
        Box::new(Hnsw::new(self.bytes_per_vec(), m, ef_construction))
    }

    fn rebuild_from_entries(&self, entries: &[(Vec<u8>, RowId)]) -> Box<dyn AnnBackend> {
        let (m, ef_construction) = self.options();
        let mut graph = Hnsw::new(self.bytes_per_vec(), m, ef_construction);
        for (bits, row_id) in entries {
            graph.insert(bits.clone(), *row_id);
        }
        Box::new(graph)
    }

    fn clone_box(&self) -> Box<dyn AnnBackend> {
        Box::new(self.clone())
    }
}

// ── DenseHnsw (Dense / cosine) backend ───────────────────────────────────────

impl AnnBackend for DenseHnsw {
    fn metric(&self) -> BackendMetric {
        BackendMetric::Cosine
    }

    fn len(&self) -> usize {
        DenseHnsw::len(self)
    }

    fn insert_validated(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        let mut on_checkpoint = || -> Result<()> { checkpoint() };
        self.insert_with_checkpoint(vec.to_vec(), row_id, &mut on_checkpoint)
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, f64)>> {
        Ok(self
            .search_with_context(query, k, ef, context)?
            .into_iter()
            .map(|(row_id, distance)| (row_id, f64::from(distance)))
            .collect())
    }

    fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        DenseHnsw::entries(self)
            .map(|(vec, row_id)| (vec_to_le_bytes(&vec), row_id))
            .collect()
    }

    fn freeze(&self) -> AnnBackendCheckpoint {
        AnnBackendCheckpoint::HnswDense {
            graph: self.clone(),
        }
    }

    fn empty_active(&self) -> Box<dyn AnnBackend> {
        let (m, ef_construction) = self.options();
        Box::new(DenseHnsw::new(self.dim(), m, ef_construction))
    }

    fn rebuild_from_entries(&self, entries: &[(Vec<u8>, RowId)]) -> Box<dyn AnnBackend> {
        let (m, ef_construction) = self.options();
        let mut graph = DenseHnsw::new(self.dim(), m, ef_construction);
        for (bytes, row_id) in entries {
            let vec = le_bytes_to_vec(bytes, self.dim());
            graph.insert(vec, *row_id);
        }
        Box::new(graph)
    }

    fn clone_box(&self) -> Box<dyn AnnBackend> {
        Box::new(self.clone())
    }
}

fn vec_to_le_bytes(vec: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vec.len() * 4);
    for value in vec {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn le_bytes_to_vec(bytes: &[u8], dim: usize) -> Vec<f32> {
    debug_assert_eq!(bytes.len(), dim * 4, "dense checkpoint entry size mismatch");
    (0..dim)
        .map(|i| {
            let offset = i * 4;
            f32::from_le_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
            ])
        })
        .collect()
}
