//! Product-quantized ANN backend.
//!
//! Stores vectors as compact PQ codes (one byte per subvector at 8 bits) and
//! ranks candidates by asymmetric distance computation (ADC) over a trained
//! codebook. This is the "flat PQ" formulation (FAISS `IndexPQ`): search is a
//! bounded scan of all codes against the query's ADC lookup table, optionally
//! followed by exact rerank over reconstructed approximations.
//!
//! Flat PQ is correct and bounded: recall depends only on codebook quality
//! (subvector count, training data), not on graph structure, so it is the
//! natural baseline for PQ. Graph-accelerated PQ (HNSW or IVF over codes)
//! composes on top of this representation in a later phase.
//!
//! ## Determinism
//!
//! The codebook is trained deterministically (see [`super::product`]); the
//! code array is keyed by [`RowId`] and iterated in sorted order, so the same
//! inputs produce the same checkpoint and the same search results. Equal ADC
//! distances break ties by `RowId`, matching the orchestrator's contract.
//!
//! ## Checkpoint
//!
//! The codebook + codes serialize into [`AnnBackendCheckpoint::Product`].
//! Encrypted tables get the whole record encrypted by the existing
//! `_idx/global.idx` GCM envelope — no new crypto path.

use crate::index::ann::backend::{AnnBackend, AnnBackendCheckpoint, BackendMetric};
use crate::index::ann::product::ProductQuantizer;
use crate::query::AiExecutionContext;
use crate::rowid::RowId;
use crate::schema::ProductQuantizerOptions;
use crate::Result;
use std::collections::BTreeMap;

/// One flat-PQ backend: a trained codebook plus a `RowId`-keyed set of codes.
///
/// The **active delta** buffers full-precision Dense vectors (`pending`) so the
/// codebook can be trained from real data before freeze. When [`freeze`] runs,
/// it trains the codebook from the buffered vectors, encodes each into a PQ
/// code, and emits `codes` + `quantizer`. The frozen layer retains only the
/// compact codes (the Dense buffer is dropped), giving the memory savings PQ
/// promises. Search over a frozen layer uses ADC over codes; the active delta
/// is searched exactly (brute-force L2 over the buffered Dense vectors) so
/// recent inserts are ranked correctly before they are quantized.
#[derive(Clone)]
pub(crate) struct PqBackend {
    dim: usize,
    num_subvectors: usize,
    bits: u8,
    rerank_factor: usize,
    /// Training options retained so `empty_active`/`rebuild_from_entries`
    /// construct compatible backends.
    training: ProductQuantizerOptions,
    /// `None` for the active delta (codebook trained lazily at freeze); `Some`
    /// for a frozen layer that carries its trained codebook.
    quantizer: Option<ProductQuantizer>,
    /// RowId -> PQ code. Populated for frozen layers; empty for a fresh active
    /// delta (which buffers Dense vectors in `pending` instead).
    codes: BTreeMap<RowId, Vec<u8>>,
    /// Buffered Dense vectors for the active delta, keyed by RowId. Drained at
    /// freeze. Empty for frozen layers.
    pending: BTreeMap<RowId, Vec<f32>>,
}

impl PqBackend {
    /// Build a fresh empty active delta. The codebook is trained at freeze
    /// from the buffered vectors.
    pub(crate) fn new(
        dim: usize,
        num_subvectors: usize,
        bits: u8,
        options: &ProductQuantizerOptions,
    ) -> Self {
        Self {
            dim,
            num_subvectors,
            bits,
            rerank_factor: options.rerank_factor,
            training: options.clone(),
            quantizer: None,
            codes: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    /// Train the codebook from the buffered Dense vectors, encode each, and
    /// return the frozen representation. Returns `None` if no vectors were
    /// buffered (empty freeze).
    fn freeze_active(&self) -> Option<(ProductQuantizer, BTreeMap<RowId, Vec<u8>>)> {
        if self.pending.is_empty() {
            return None;
        }
        let samples: Vec<&[f32]> = self.pending.values().map(|v| v.as_slice()).collect();
        let quantizer = ProductQuantizer::train(
            self.dim,
            self.num_subvectors,
            self.bits,
            &samples,
            &self.training,
        )?;
        let mut codes = BTreeMap::new();
        for (row_id, vec) in &self.pending {
            codes.insert(*row_id, quantizer.encode(vec));
        }
        Some((quantizer, codes))
    }

    /// Reconstruct a backend directly from a checkpoint payload (codebook +
    /// codes), bypassing training. Used by `AnnIndex::from_checkpoint`.
    pub(crate) fn from_checkpoint(
        dim: usize,
        num_subvectors: usize,
        bits: u8,
        rerank_factor: usize,
        quantizer: ProductQuantizer,
        codes: BTreeMap<RowId, Vec<u8>>,
    ) -> Self {
        let training = ProductQuantizerOptions {
            seed: quantizer.seed(),
            rerank_factor,
            ..ProductQuantizerOptions::default()
        };
        Self {
            dim,
            num_subvectors,
            bits,
            rerank_factor,
            training,
            quantizer: Some(quantizer),
            codes,
            pending: BTreeMap::new(),
        }
    }

    fn k(&self) -> usize {
        1usize << self.bits
    }
}

impl AnnBackend for PqBackend {
    fn metric(&self) -> BackendMetric {
        // ADC distance is a squared-L2 approximation; reported as Cosine so
        // the orchestrator's distance wrapper treats it as an f32 metric.
        BackendMetric::Cosine
    }

    fn len(&self) -> usize {
        self.codes.len() + self.pending.len()
    }

    fn is_empty(&self) -> bool {
        self.codes.is_empty() && self.pending.is_empty()
    }

    fn insert_validated(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        _checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        // The active delta buffers the Dense vector; the codebook is trained
        // from buffered vectors at freeze. A frozen layer rejects inserts
        // (the orchestrator only inserts into the active delta).
        if self.quantizer.is_some() {
            // Frozen layer receiving an insert: treat as a fresh active delta
            // by clearing codes/quantizer and buffering. This path is not
            // exercised by the orchestrator (it seals before re-inserting) but
            // keeps the backend self-consistent under direct use.
            self.quantizer = None;
            self.codes.clear();
        }
        self.pending.insert(row_id, vec.to_vec());
        Ok(())
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        _ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, f64)>> {
        // Active delta: exact brute-force L2 over buffered Dense vectors.
        let mut scored: Vec<(f32, RowId)> = Vec::new();
        if !self.pending.is_empty() {
            for (i, (row_id, vec)) in self.pending.iter().enumerate() {
                if let Some(context) = context {
                    if i.is_multiple_of(64) {
                        context.checkpoint()?;
                    }
                }
                scored.push((squared_l2(query, vec), *row_id));
            }
        }
        // Frozen layer: ADC over codes against the query lookup table.
        if let Some(quantizer) = &self.quantizer {
            if !self.codes.is_empty() {
                let table = quantizer.adc_table(query);
                let k_codes = self.k();
                for (i, (row_id, code)) in self.codes.iter().enumerate() {
                    if let Some(context) = context {
                        if i.is_multiple_of(64) {
                            context.checkpoint()?;
                        }
                    }
                    let dist =
                        ProductQuantizer::adc_distance(&table, code, self.num_subvectors, k_codes);
                    scored.push((dist, *row_id));
                }
            }
        }
        if scored.is_empty() {
            return Ok(Vec::new());
        }
        // Ascending distance, ties by RowId.
        scored.sort_by(|(da, ra), (db, rb)| da.total_cmp(db).then_with(|| ra.cmp(rb)));
        // Optional exact rerank over reconstructed approximations for the
        // frozen-layer candidates (active-delta candidates are already exact).
        let rerank_set = if self.rerank_factor > 0 {
            (k.saturating_mul(self.rerank_factor)).min(scored.len())
        } else {
            k.min(scored.len())
        };
        if self.rerank_factor > 0 && rerank_set > k {
            if let Some(quantizer) = &self.quantizer {
                let mut reranked: Vec<(f32, RowId)> = scored[..rerank_set]
                    .iter()
                    .map(|(_, row_id)| {
                        // Active-delta entries are exact already; only
                        // frozen-layer (coded) entries benefit from rerank.
                        if let Some(vec) = self.pending.get(row_id) {
                            (squared_l2(query, vec), *row_id)
                        } else if let Some(code) = self.codes.get(row_id) {
                            let recon = quantizer.reconstruct(code);
                            (squared_l2(query, &recon), *row_id)
                        } else {
                            (f32::INFINITY, *row_id)
                        }
                    })
                    .collect();
                reranked.sort_by(|(da, ra), (db, rb)| da.total_cmp(db).then_with(|| ra.cmp(rb)));
                return Ok(reranked
                    .into_iter()
                    .take(k)
                    .map(|(dist, row_id)| (row_id, f64::from(dist)))
                    .collect());
            }
        }
        Ok(scored
            .into_iter()
            .take(k)
            .map(|(dist, row_id)| (row_id, f64::from(dist)))
            .collect())
    }

    fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        // Native representation carries the full-precision Dense vector so
        // consolidation can retrain a fresh codebook from the union. Format:
        //   [8 bytes RowId le][4-byte dim f32 little-endian vector]
        let mut out: Vec<(Vec<u8>, RowId)> = Vec::new();
        for (row_id, vec) in &self.pending {
            let mut bytes = Vec::with_capacity(8 + vec.len() * 4);
            bytes.extend_from_slice(&row_id.0.to_le_bytes());
            for value in vec {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            out.push((bytes, *row_id));
        }
        for (row_id, code) in &self.codes {
            if let Some(quantizer) = &self.quantizer {
                let recon = quantizer.reconstruct(code);
                let mut bytes = Vec::with_capacity(8 + recon.len() * 4);
                bytes.extend_from_slice(&row_id.0.to_le_bytes());
                for value in &recon {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                out.push((bytes, *row_id));
            }
        }
        out
    }

    fn freeze(&self) -> AnnBackendCheckpoint {
        // If this is an active delta with buffered vectors, train + encode now.
        let (quantizer, codes) = if let Some(quantizer) = &self.quantizer {
            (quantizer.clone(), self.codes.clone())
        } else if let Some((quantizer, codes)) = self.freeze_active() {
            (quantizer, codes)
        } else {
            // Empty freeze: emit a zero-trained codebook so the checkpoint is
            // well-formed (the orchestrator skips empty seals).
            let zero = vec![0.0f32; self.dim];
            let quantizer = ProductQuantizer::train(
                self.dim,
                self.num_subvectors,
                self.bits,
                &[zero.as_slice()],
                &self.training,
            )
            .expect("non-empty training set");
            (quantizer, BTreeMap::new())
        };
        AnnBackendCheckpoint::Product {
            dim: self.dim,
            num_subvectors: self.num_subvectors,
            bits: self.bits,
            rerank_factor: self.rerank_factor,
            quantizer,
            codes,
        }
    }

    fn empty_active(&self) -> Box<dyn AnnBackend> {
        Box::new(Self::new(
            self.dim,
            self.num_subvectors,
            self.bits,
            &self.training,
        ))
    }

    fn rebuild_from_entries(&self, entries: &[(Vec<u8>, RowId)]) -> Box<dyn AnnBackend> {
        // Rebuild a fresh active delta from Dense-vector entries, then train +
        // encode so the consolidated frozen layer is byte-identical to a
        // single-batch build. Determinism: same entries in same order + same
        // training seed → same codebook + same codes.
        let mut pending = BTreeMap::new();
        for (bytes, _) in entries {
            if bytes.len() < 8 + self.dim * 4 {
                continue;
            }
            let mut rid_bytes = [0u8; 8];
            rid_bytes.copy_from_slice(&bytes[..8]);
            let row_id = RowId(u64::from_le_bytes(rid_bytes));
            let vec = (0..self.dim)
                .map(|i| {
                    let offset = 8 + i * 4;
                    f32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ])
                })
                .collect();
            pending.insert(row_id, vec);
        }
        let mut rebuilt = Self {
            dim: self.dim,
            num_subvectors: self.num_subvectors,
            bits: self.bits,
            rerank_factor: self.rerank_factor,
            training: self.training.clone(),
            quantizer: None,
            codes: BTreeMap::new(),
            pending,
        };
        // Train + encode immediately so the rebuilt instance is a frozen layer
        // matching a fresh build.
        if let Some((quantizer, codes)) = rebuilt.freeze_active() {
            rebuilt.quantizer = Some(quantizer);
            rebuilt.codes = codes;
            rebuilt.pending.clear();
        }
        Box::new(rebuilt)
    }

    fn clone_box(&self) -> Box<dyn AnnBackend> {
        Box::new(self.clone())
    }
}

fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = x - y;
        sum += d * d;
    }
    sum
}
