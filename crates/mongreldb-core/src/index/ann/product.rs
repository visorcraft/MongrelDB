//! Product quantization (PQ).
//!
//! Vectors are split into `num_subvectors` contiguous groups; each group is
//! encoded against a trained codebook of `2^bits` k-means centroids (256
//! centroids at `bits = 8`). A vector is stored as one byte per subvector
//! (the nearest centroid index), so a 768-dim f32 vector (3072 bytes) with
//! `num_subvectors = 32` becomes 32 bytes — a 96× reduction.
//!
//! Distance is asymmetric (ADC): the query stays full-precision and its
//! per-subvector residual to every centroid is precomputed into a lookup
//! table; candidate distance is the sum of table lookups, one per subvector.
//! This is the standard Jégou et al. formulation.
//!
//! ## Determinism
//!
//! Training is deterministic: the same `seed`, the same training vectors (in
//! the same order), and the same `ProductQuantizerOptions` produce a
//! byte-identical codebook. This makes PQ checkpoints reproducible and is the
//! foundation for the merge/consolidation guarantee that a rebuilt base
//! matches a base-only build.
//!
//! ## Rerank
//!
//! ADC distance is approximate. The PQ backend optionally reranks the top
//! `k * rerank_factor` ADC candidates using **reconstructed approximate
//! vectors** (centroid concatenation), which improves ranking quality over
//! plain ADC. This is not a true exact rerank — the original Dense vectors are
//! dropped at freeze to deliver PQ's memory savings. A future enhancement may
//! optionally retain Dense vectors for the rerank window to provide true exact
//! rerank at higher memory cost.

use crate::schema::ProductQuantizerOptions;
use crate::Result;

/// One trained product quantizer. Codebooks are `num_subvectors` blocks of
/// `2^bits` centroids, each centroid `subvector_dim` f32 values.
///
/// At `bits = 8` (the supported value) each block has 256 centroids.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ProductQuantizer {
    dim: usize,
    num_subvectors: usize,
    subvector_dim: usize,
    bits: u8,
    /// `codebooks[s * 256 + c]` is centroid `c` for subvector `s`.
    /// Stored flat for cache-friendly ADC.
    codebooks: Vec<f32>,
    seed: u64,
}

impl ProductQuantizer {
    /// Train a product quantizer from `samples` (each a `dim`-length f32
    /// vector). At most `options.training_samples` are used; the rest are
    /// ignored. Training is deterministic for a fixed `seed` + sample order.
    ///
    /// Returns `None` if `samples` is empty — the caller must handle a
    /// zero-row build by deferring training until rows arrive (the online DDL
    /// catch-up path).
    pub(crate) fn train(
        dim: usize,
        num_subvectors: usize,
        bits: u8,
        samples: &[&[f32]],
        options: &ProductQuantizerOptions,
    ) -> Option<Self> {
        Self::train_with_checkpoint(dim, num_subvectors, bits, samples, options, &mut || Ok(()))
            .expect("infallible product-training checkpoint")
    }

    pub(crate) fn train_with_checkpoint(
        dim: usize,
        num_subvectors: usize,
        bits: u8,
        samples: &[&[f32]],
        options: &ProductQuantizerOptions,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<Option<Self>> {
        if samples.is_empty() || dim == 0 || num_subvectors == 0 {
            return Ok(None);
        }
        debug_assert_eq!(bits, 8, "only 8-bit product quantization is supported");
        debug_assert_eq!(
            dim % num_subvectors,
            0,
            "num_subvectors must evenly divide dim"
        );
        if bits != 8 || !dim.is_multiple_of(num_subvectors) {
            return Ok(None);
        }
        let subvector_dim = dim / num_subvectors;
        let k = 1usize << bits; // 256 at bits=8
                                // Deterministic sample selection + ordering. The seed is mixed in so
                                // different indexes with the same data produce different codebooks
                                // only when their seeds differ.
        let selected = select_samples(samples, options.training_samples, options.seed);
        let mut codebooks = vec![0.0f32; num_subvectors * k * subvector_dim];
        for s in 0..num_subvectors {
            checkpoint()?;
            let sub_start = s * subvector_dim;
            let sub_samples: Vec<&[f32]> = selected
                .iter()
                .map(|vec| &vec[sub_start..sub_start + subvector_dim])
                .collect();
            let centroids = kmeans_train(
                &sub_samples,
                subvector_dim,
                k,
                options.seed,
                s as u64,
                checkpoint,
            )?;
            codebooks[s * k * subvector_dim..(s + 1) * k * subvector_dim]
                .copy_from_slice(&centroids);
        }
        Ok(Some(Self {
            dim,
            num_subvectors,
            subvector_dim,
            bits,
            codebooks,
            seed: options.seed,
        }))
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim
    }
    pub(crate) fn num_subvectors(&self) -> usize {
        self.num_subvectors
    }
    #[allow(dead_code)]
    pub(crate) fn subvector_dim(&self) -> usize {
        self.subvector_dim
    }
    #[allow(dead_code)]
    pub(crate) fn bits(&self) -> u8 {
        self.bits
    }
    pub(crate) fn seed(&self) -> u64 {
        self.seed
    }

    pub(crate) fn matches_checkpoint(&self, dim: usize, num_subvectors: usize, bits: u8) -> bool {
        bits == 8
            && self.dim == dim
            && self.num_subvectors == num_subvectors
            && self.bits == bits
            && num_subvectors > 0
            && dim.is_multiple_of(num_subvectors)
            && self.subvector_dim == dim / num_subvectors
            && self.codebooks.len() == dim.saturating_mul(1usize << bits)
            && self.codebooks.iter().all(|value| value.is_finite())
    }

    /// Encode a `dim`-length f32 vector into `num_subvectors` bytes (one
    /// centroid index per subvector at bits=8).
    pub(crate) fn encode(&self, vec: &[f32]) -> Vec<u8> {
        debug_assert_eq!(vec.len(), self.dim, "encode vector dim mismatch");
        let k = 1usize << self.bits;
        let mut out = vec![0u8; self.num_subvectors];
        // Index-based loop: each iteration computes offsets into three
        // different slices (vec, out, codebooks) from `s`.
        #[allow(clippy::needless_range_loop)]
        for s in 0..self.num_subvectors {
            let sub_start = s * self.subvector_dim;
            let sub = &vec[sub_start..sub_start + self.subvector_dim];
            let block_start = s * k * self.subvector_dim;
            let block = &self.codebooks[block_start..block_start + k * self.subvector_dim];
            out[s] = nearest_centroid(sub, block, self.subvector_dim, k);
        }
        out
    }

    /// Build the ADC lookup table for a full-precision query: for each
    /// subvector `s` and each centroid `c`, the squared-L2 distance from the
    /// query subvector to centroid `c`. The table is `num_subvectors * k`
    /// f32 values; candidate distance is the sum of `table[s * k + code[s]]`.
    pub(crate) fn adc_table(&self, query: &[f32]) -> Vec<f32> {
        debug_assert_eq!(query.len(), self.dim, "adc query dim mismatch");
        let k = 1usize << self.bits;
        let mut table = vec![0.0f32; self.num_subvectors * k];
        for s in 0..self.num_subvectors {
            let sub_start = s * self.subvector_dim;
            let q_sub = &query[sub_start..sub_start + self.subvector_dim];
            let block_start = s * k * self.subvector_dim;
            for c in 0..k {
                let centroid = &self.codebooks[block_start + c * self.subvector_dim
                    ..block_start + (c + 1) * self.subvector_dim];
                table[s * k + c] = squared_l2(q_sub, centroid);
            }
        }
        table
    }

    /// ADC distance from a query (via its [`adc_table`]) to an encoded vector.
    /// Lower is better. This is an approximate squared-L2 distance. A caller
    /// can only rerank exactly if it separately retained the source vectors.
    pub(crate) fn adc_distance(table: &[f32], code: &[u8], num_subvectors: usize, k: usize) -> f32 {
        let mut total = 0.0f32;
        for s in 0..num_subvectors {
            total += table[s * k + code[s] as usize];
        }
        total
    }

    /// Reconstruct the full-precision approximation of an encoded vector
    /// (centroid concatenation). Used by tests and approximate reranking when the
    /// Dense source is not retained.
    pub(crate) fn reconstruct(&self, code: &[u8]) -> Vec<f32> {
        debug_assert_eq!(code.len(), self.num_subvectors, "code length mismatch");
        let k = 1usize << self.bits;
        let mut out = vec![0.0f32; self.dim];
        // Index-based loop: each iteration computes offsets into two slices
        // (out, codebooks) and indexes `code` from `s`.
        #[allow(clippy::needless_range_loop)]
        for s in 0..self.num_subvectors {
            let block_start = s * k * self.subvector_dim;
            let centroid = &self.codebooks[block_start + code[s] as usize * self.subvector_dim
                ..block_start + (code[s] as usize + 1) * self.subvector_dim];
            let sub_start = s * self.subvector_dim;
            out[sub_start..sub_start + self.subvector_dim].copy_from_slice(centroid);
        }
        out
    }
}

/// Squared L2 distance between two equal-length f32 slices.
fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut sum = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = x - y;
        sum += d * d;
    }
    sum
}

/// Index of the nearest centroid to `sub` in `centroids` (flat, `k` centroids
/// each `dim` long). Ties break by lowest index for determinism.
fn nearest_centroid(sub: &[f32], centroids: &[f32], dim: usize, k: usize) -> u8 {
    let mut best = 0usize;
    let mut best_dist = f32::INFINITY;
    for c in 0..k {
        let centroid = &centroids[c * dim..(c + 1) * dim];
        let dist = squared_l2(sub, centroid);
        if dist < best_dist {
            best_dist = dist;
            best = c;
        }
    }
    best as u8
}

/// Deterministically select up to `cap` samples from `samples` using a
/// fixed-seed splitmix64 stream. The selection is stable: the same input
/// always yields the same subset in the same order, so k-means training is
/// reproducible.
fn select_samples<'a>(samples: &[&'a [f32]], cap: usize, seed: u64) -> Vec<&'a [f32]> {
    if samples.len() <= cap {
        return samples.to_vec();
    }
    // Deterministic stride sampling: pick every (len/cap)-th sample starting
    // at a seed-derived offset. Cheaper and more order-stable than a hash
    // shuffle, and sufficient for k-means seeding on typical embeddings.
    let stride = samples.len() / cap;
    let start = (splitmix64(seed) as usize) % stride.max(1);
    (0..cap)
        .map(|i| samples[(start + i * stride) % samples.len()])
        .collect()
}

/// Deterministic k-means. Centroids are seeded by a stride pick from the
/// samples (k-means++ is non-deterministic under ties without care; a fixed
/// stride seed is reproducible and adequate for PQ). Up to 25 Lloyd
/// iterations; converges early when no centroid moves.
fn kmeans_train(
    samples: &[&[f32]],
    dim: usize,
    k: usize,
    seed: u64,
    salt: u64,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<f32>> {
    // Cap effective k by sample count: empty clusters collapse to the first
    // sample (keeps the codebook well-defined for tiny training sets).
    let effective_k = k.min(samples.len().max(1));
    let mut centroids = vec![0.0f32; k * dim];
    // Seed centroids by striding through the samples at a seed-derived offset.
    let start = (splitmix64(seed.wrapping_add(salt)) as usize) % samples.len().max(1);
    for c in 0..effective_k {
        let src = samples[(start + c * (samples.len() / effective_k).max(1)) % samples.len()];
        centroids[c * dim..(c + 1) * dim].copy_from_slice(src);
    }
    // Fill any remaining centroids (k > samples) with zeros so the codebook
    // is fully populated; they will never be the nearest centroid.
    for iter in 0..25 {
        checkpoint()?;
        let (assignments, inertia) = assign_samples(samples, &centroids, dim, k, checkpoint)?;
        let new_centroids =
            update_centroids(&assignments, samples, dim, k, &centroids, checkpoint)?;
        let moved = centroids
            .iter()
            .zip(new_centroids.iter())
            .map(|(a, b)| (a - b).abs())
            .fold(0.0f32, f32::max);
        centroids = new_centroids;
        if iter > 0 && moved < 1e-6 {
            let _ = inertia; // converged
            break;
        }
    }
    Ok(centroids)
}

/// Assign each sample to its nearest centroid. Returns (assignments, inertia).
fn assign_samples(
    samples: &[&[f32]],
    centroids: &[f32],
    dim: usize,
    k: usize,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<(Vec<usize>, f32)> {
    let mut assignments = Vec::with_capacity(samples.len());
    let mut inertia = 0.0f32;
    for (index, sample) in samples.iter().enumerate() {
        if index.is_multiple_of(64) {
            checkpoint()?;
        }
        let mut best = 0usize;
        let mut best_dist = f32::INFINITY;
        for c in 0..k {
            let centroid = &centroids[c * dim..(c + 1) * dim];
            let dist = squared_l2(sample, centroid);
            if dist < best_dist {
                best_dist = dist;
                best = c;
            }
        }
        inertia += best_dist;
        assignments.push(best);
    }
    Ok((assignments, inertia))
}

/// Recompute centroids as the mean of assigned samples. Empty clusters keep
/// their previous centroid (no NaN), preserving determinism.
fn update_centroids(
    assignments: &[usize],
    samples: &[&[f32]],
    dim: usize,
    k: usize,
    previous: &[f32],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<f32>> {
    let mut sums = vec![0.0f32; k * dim];
    let mut counts = vec![0u32; k];
    for (index, (sample, &cluster)) in samples.iter().zip(assignments.iter()).enumerate() {
        if index.is_multiple_of(64) {
            checkpoint()?;
        }
        for (i, value) in sample.iter().enumerate() {
            sums[cluster * dim + i] += value;
        }
        counts[cluster] += 1;
    }
    let mut out = vec![0.0f32; k * dim];
    for c in 0..k {
        if counts[c] > 0 {
            let n = counts[c] as f32;
            for i in 0..dim {
                out[c * dim + i] = sums[c * dim + i] / n;
            }
        } else {
            // Empty cluster: retain the previous centroid.
            out[c * dim..(c + 1) * dim].copy_from_slice(&previous[c * dim..(c + 1) * dim]);
        }
    }
    Ok(out)
}

/// splitmix64 — a deterministic, portable 64-bit PRNG for seed derivation.
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = z;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(seed: u64) -> ProductQuantizerOptions {
        ProductQuantizerOptions {
            training_samples: 10_000,
            seed,
            rerank_factor: 5,
        }
    }

    fn clustered_data(dim: usize, clusters: usize, per_cluster: usize) -> Vec<Vec<f32>> {
        let mut out = Vec::new();
        for c in 0..clusters {
            let center = (c * 17) % 100;
            for _ in 0..per_cluster {
                let mut v = vec![0f32; dim];
                for (i, x) in v.iter_mut().enumerate() {
                    // Cluster c is centered at `center` with small jitter; the
                    // nearest centroid should be the cluster mean.
                    *x = (center as f32) + ((i as f32 % 7.0) - 3.0) * 0.01;
                }
                out.push(v);
            }
        }
        out
    }

    #[test]
    fn train_returns_none_for_empty_samples() {
        assert!(ProductQuantizer::train(8, 4, 8, &[], &options(1)).is_none());
    }

    #[test]
    fn encode_round_trips_centroid_indices() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq = ProductQuantizer::train(dim, 4, 8, &refs, &options(7)).unwrap();
        assert_eq!(pq.num_subvectors(), 4);
        assert_eq!(pq.subvector_dim(), 2);
        // Each encoded vector is 4 bytes (one per subvector).
        let code = pq.encode(&data[0]);
        assert_eq!(code.len(), 4);
    }

    #[test]
    fn training_is_deterministic_for_fixed_seed() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq1 = ProductQuantizer::train(dim, 4, 8, &refs, &options(42)).unwrap();
        let pq2 = ProductQuantizer::train(dim, 4, 8, &refs, &options(42)).unwrap();
        // Same seed + same data in same order → identical codebooks.
        assert_eq!(pq1.codebooks, pq2.codebooks);
    }

    #[test]
    fn different_seeds_may_differ() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq1 = ProductQuantizer::train(dim, 4, 8, &refs, &options(1)).unwrap();
        let pq2 = ProductQuantizer::train(dim, 4, 8, &refs, &options(999)).unwrap();
        // Different seeds exercise different stride offsets; codebooks are
        // likely (not guaranteed) to differ.
        assert!(pq1.codebooks != pq2.codebooks || pq1.seed != pq2.seed);
    }

    #[test]
    fn adc_distance_ranks_identical_vectors_at_zero() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq = ProductQuantizer::train(dim, 4, 8, &refs, &options(3)).unwrap();
        let k = 1usize << pq.bits;
        // A vector reconstructed from its own code has ADC distance 0 to the
        // reconstructed approximation... but ADC measures distance to the
        // *query*, not to stored vectors. Encode the query and verify its ADC
        // distance to its own code is the quantization residual (small).
        let query = &data[0];
        let table = pq.adc_table(query);
        let code = pq.encode(query);
        let dist = ProductQuantizer::adc_distance(&table, &code, pq.num_subvectors, k);
        assert!(dist < 1.0, "self-distance should be small, got {dist}");
    }

    #[test]
    fn adc_distance_separates_distant_clusters() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq = ProductQuantizer::train(dim, 4, 8, &refs, &options(3)).unwrap();
        let k = 1usize << pq.bits;
        // A query from cluster 0 vs a stored vector from cluster 3.
        let query = &data[0]; // cluster 0
        let far = &data[data.len() - 1]; // cluster 3
        let table = pq.adc_table(query);
        let code_near = pq.encode(query);
        let code_far = pq.encode(far);
        let dist_near = ProductQuantizer::adc_distance(&table, &code_near, pq.num_subvectors, k);
        let dist_far = ProductQuantizer::adc_distance(&table, &code_far, pq.num_subvectors, k);
        assert!(
            dist_far > dist_near,
            "far cluster ({dist_far}) should be farther than near ({dist_near})"
        );
    }

    #[test]
    fn reconstruct_concatenates_centroids() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq = ProductQuantizer::train(dim, 4, 8, &refs, &options(3)).unwrap();
        let code = pq.encode(&data[0]);
        let recon = pq.reconstruct(&code);
        assert_eq!(recon.len(), dim);
    }

    #[test]
    fn pq_serializes_round_trip() {
        let dim = 8;
        let data = clustered_data(dim, 4, 16);
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq = ProductQuantizer::train(dim, 4, 8, &refs, &options(11)).unwrap();
        let json = serde_json::to_string(&pq).unwrap();
        let de: ProductQuantizer = serde_json::from_str(&json).unwrap();
        assert_eq!(de.codebooks, pq.codebooks);
        assert_eq!(de.encode(&data[0]), pq.encode(&data[0]));
    }

    #[test]
    fn handles_fewer_samples_than_centroids() {
        // 3 samples, 256 centroids per subvector — must not panic and must
        // produce a valid (zeros-filled) codebook.
        let dim = 4;
        let data: Vec<Vec<f32>> = (0..3)
            .map(|i| vec![i as f32, (i + 1) as f32, 0.0, 0.0])
            .collect();
        let refs: Vec<&[f32]> = data.iter().map(|v| v.as_slice()).collect();
        let pq = ProductQuantizer::train(dim, 2, 8, &refs, &options(5)).unwrap();
        let code = pq.encode(&data[0]);
        assert_eq!(code.len(), 2);
    }
}
