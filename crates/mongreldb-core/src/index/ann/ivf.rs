//! Inverted-file (IVF) backend.
//!
//! Partitions the vector space into `nlist` cells via k-means; each vector is
//! assigned to its nearest centroid and stored in that centroid's inverted
//! list. Search finds the `nprobe` nearest centroids to the query, scans their
//! lists, and reranks candidates exactly. This is the standard IVF structure
//! (Jégou et al.); recall is governed by `nlist`/`nprobe` and the centroid
//! quality.
//!
//! ## Determinism
//!
//! Centroids are trained by a fixed-seed stride-seeded k-means (same approach
//! as product quantization); inverted lists are keyed by centroid id and
//! iterate in insertion order, so the same vectors produce a byte-identical
//! checkpoint and search results. Equal distances break ties by `RowId`.
//!
//! ## Representation
//!
//! Dense vectors are stored full-precision (cosine distance). The active delta
//! buffers vectors and retrains centroids at freeze (mirroring the PQ backend
//! design), so the published centroids reflect the actual data distribution.

use crate::index::ann::backend::{AnnBackend, AnnBackendCheckpoint, BackendMetric};
use crate::index::hnsw::cosine_distance;
use crate::query::AiExecutionContext;
use crate::rowid::RowId;
use crate::schema::IvfOptions;
use crate::Result;
use std::collections::BTreeMap;

/// One IVF backend: trained centroids + per-centroid inverted lists.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct IvfBackend {
    dim: usize,
    nlist: usize,
    nprobe: usize,
    /// Cap on training samples for k-means centroid training. Bounds the
    /// O(iterations × samples × nlist × dim) training cost.
    training_samples: usize,
    /// `None` for the active delta (centroids trained at freeze); `Some` for a
    /// frozen layer.
    centroids: Option<Vec<Vec<f32>>>,
    /// Centroid id -> inverted list of (RowId, vector). Frozen layers carry
    /// the vectors for exact rerank during search.
    lists: BTreeMap<usize, Vec<(RowId, Vec<f32>)>>,
    /// Buffered vectors for the active delta, keyed by RowId. Drained at freeze.
    pending: BTreeMap<RowId, Vec<f32>>,
    seed: u64,
}

/// Trained centroids + per-cell lists produced by [`IvfBackend::freeze_active`].
type FrozenIvf = (Vec<Vec<f32>>, BTreeMap<usize, Vec<(RowId, Vec<f32>)>>);

impl IvfBackend {
    pub(crate) fn new(dim: usize, options: &IvfOptions, seed: u64) -> Self {
        Self {
            dim,
            nlist: options.nlist,
            nprobe: options.nprobe,
            training_samples: options.training_samples,
            centroids: None,
            lists: BTreeMap::new(),
            pending: BTreeMap::new(),
            seed,
        }
    }

    /// Train centroids from buffered vectors, assign each to its nearest
    /// centroid, and return the frozen representation. Training samples are
    /// capped by `training_samples` to bound k-means cost.
    fn freeze_active(&self) -> Option<FrozenIvf> {
        if self.pending.is_empty() {
            return None;
        }
        // Cap training samples to bound the O(iterations × N × nlist × dim)
        // k-means cost. Uses deterministic stride sampling matching the PQ
        // backend's approach.
        let all_samples: Vec<&[f32]> = self.pending.values().map(|v| v.as_slice()).collect();
        let samples: Vec<&[f32]> = if all_samples.len() > self.training_samples {
            let stride = all_samples.len() / self.training_samples;
            let start = (splitmix64(self.seed) as usize) % stride.max(1);
            (0..self.training_samples)
                .map(|i| all_samples[(start + i * stride) % all_samples.len()])
                .collect()
        } else {
            all_samples
        };
        // Cap effective nlist by sample count so tiny training sets don't
        // produce empty cells.
        let effective_nlist = self.nlist.min(samples.len());
        let centroids = kmeans(&samples, self.dim, effective_nlist, self.seed);
        let mut lists: BTreeMap<usize, Vec<(RowId, Vec<f32>)>> = BTreeMap::new();
        for (row_id, vec) in &self.pending {
            let cell = nearest_centroid(vec, &centroids);
            lists.entry(cell).or_default().push((*row_id, vec.clone()));
        }
        Some((centroids, lists))
    }

    pub(crate) fn from_checkpoint(
        dim: usize,
        nlist: usize,
        nprobe: usize,
        centroids: Vec<Vec<f32>>,
        lists: BTreeMap<usize, Vec<(RowId, Vec<f32>)>>,
        seed: u64,
    ) -> Self {
        Self {
            dim,
            nlist,
            nprobe,
            training_samples: IvfOptions::default().training_samples,
            centroids: Some(centroids),
            lists,
            pending: BTreeMap::new(),
            seed,
        }
    }
}

impl AnnBackend for IvfBackend {
    fn metric(&self) -> BackendMetric {
        BackendMetric::Cosine
    }

    fn len(&self) -> usize {
        self.lists.values().map(|l| l.len()).sum::<usize>() + self.pending.len()
    }

    fn is_empty(&self) -> bool {
        self.lists.values().all(|l| l.is_empty()) && self.pending.is_empty()
    }

    fn insert_validated(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        _checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        // Active delta buffers; frozen layer clears and re-buffers (not
        // exercised by the orchestrator, which seals before re-inserting).
        if self.centroids.is_some() {
            self.centroids = None;
            self.lists.clear();
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
        let mut scored: Vec<(f32, RowId)> = Vec::new();
        // Frozen layer: probe the nprobe nearest centroids' lists.
        if let Some(centroids) = &self.centroids {
            if !centroids.is_empty() {
                // Rank centroids by distance to the query, take nprobe nearest.
                let mut centroid_dists: Vec<(f32, usize)> = centroids
                    .iter()
                    .enumerate()
                    .map(|(i, c)| (cosine_distance(query, c), i))
                    .collect();
                centroid_dists.sort_by(|(da, _), (db, _)| da.total_cmp(db));
                let probes = self.nprobe.min(centroids.len());
                for (_, cell) in centroid_dists.into_iter().take(probes) {
                    if let Some(list) = self.lists.get(&cell) {
                        for (i, (row_id, vec)) in list.iter().enumerate() {
                            if let Some(context) = context {
                                if i.is_multiple_of(64) {
                                    context.checkpoint()?;
                                }
                            }
                            scored.push((cosine_distance(query, vec), *row_id));
                        }
                    }
                }
            }
        }
        // Active delta: exact brute force (no centroids trained yet).
        for (i, (row_id, vec)) in self.pending.iter().enumerate() {
            if let Some(context) = context {
                if i.is_multiple_of(64) {
                    context.checkpoint()?;
                }
            }
            scored.push((cosine_distance(query, vec), *row_id));
        }
        scored.sort_by(|(da, ra), (db, rb)| da.total_cmp(db).then_with(|| ra.cmp(rb)));
        Ok(scored
            .into_iter()
            .take(k)
            .map(|(dist, row_id)| (row_id, f64::from(dist)))
            .collect())
    }

    fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        let mut out: Vec<(Vec<u8>, RowId)> = Vec::new();
        for (row_id, vec) in &self.pending {
            out.push((vec_to_bytes(vec), *row_id));
        }
        for list in self.lists.values() {
            for (row_id, vec) in list {
                out.push((vec_to_bytes(vec), *row_id));
            }
        }
        out
    }

    fn freeze(&self) -> AnnBackendCheckpoint {
        let (centroids, lists) = if let Some(centroids) = &self.centroids {
            (centroids.clone(), self.lists.clone())
        } else if let Some((centroids, lists)) = self.freeze_active() {
            (centroids, lists)
        } else {
            // Empty freeze: emit a single zero centroid so the checkpoint is
            // well-formed.
            (vec![vec![0.0f32; self.dim]], BTreeMap::new())
        };
        AnnBackendCheckpoint::Ivf {
            dim: self.dim,
            nlist: self.nlist,
            nprobe: self.nprobe,
            centroids,
            lists,
            seed: self.seed,
        }
    }

    fn empty_active(&self) -> Box<dyn AnnBackend> {
        Box::new(Self::new(
            self.dim,
            &IvfOptions {
                nlist: self.nlist,
                nprobe: self.nprobe,
                training_samples: self.training_samples,
            },
            self.seed,
        ))
    }

    fn rebuild_from_entries(&self, entries: &[(Vec<u8>, RowId)]) -> Box<dyn AnnBackend> {
        let mut pending = BTreeMap::new();
        for (bytes, row_id) in entries {
            if bytes.len() >= self.dim * 4 {
                pending.insert(*row_id, vec_from_bytes(bytes, self.dim));
            }
        }
        // Rebuild as a frozen layer: train centroids from the entries.
        let mut rebuilt = Self {
            dim: self.dim,
            nlist: self.nlist,
            nprobe: self.nprobe,
            training_samples: self.training_samples,
            centroids: None,
            lists: BTreeMap::new(),
            pending,
            seed: self.seed,
        };
        if let Some((centroids, lists)) = rebuilt.freeze_active() {
            rebuilt.centroids = Some(centroids);
            rebuilt.lists = lists;
            rebuilt.pending.clear();
        }
        Box::new(rebuilt)
    }

    fn clone_box(&self) -> Box<dyn AnnBackend> {
        Box::new(self.clone())
    }
}

/// Full-vector k-means. Stride-seeded centroids (deterministic), up to 25
/// Lloyd iterations, empty clusters retain their previous centroid.
fn kmeans(samples: &[&[f32]], dim: usize, k: usize, seed: u64) -> Vec<Vec<f32>> {
    if samples.is_empty() {
        return vec![vec![0.0f32; dim]];
    }
    let effective_k = k.min(samples.len()).max(1);
    let mut centroids = vec![vec![0.0f32; dim]; k];
    let start = (splitmix64(seed) as usize) % samples.len();
    for c in 0..effective_k {
        let src = samples[(start + c * (samples.len() / effective_k).max(1)) % samples.len()];
        centroids[c] = src.to_vec();
    }
    for _iter in 0..25 {
        let mut sums = vec![vec![0.0f32; dim]; k];
        let mut counts = vec![0u32; k];
        for sample in samples {
            let nearest = nearest_centroid(sample, &centroids);
            for (i, value) in sample.iter().enumerate() {
                sums[nearest][i] += value;
            }
            counts[nearest] += 1;
        }
        let mut moved = 0.0f32;
        for c in 0..k {
            if counts[c] > 0 {
                let n = counts[c] as f32;
                for i in 0..dim {
                    let new = sums[c][i] / n;
                    moved = moved.max((centroids[c][i] - new).abs());
                    centroids[c][i] = new;
                }
            }
        }
        if moved < 1e-6 {
            break;
        }
    }
    centroids
}

/// Index of the nearest centroid to `vec`.
fn nearest_centroid(vec: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best = 0usize;
    let mut best_dist = f32::INFINITY;
    for (i, c) in centroids.iter().enumerate() {
        let dist = cosine_distance(vec, c);
        if dist < best_dist {
            best_dist = dist;
            best = i;
        }
    }
    best
}

fn vec_to_bytes(vec: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(vec.len() * 4);
    for value in vec {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn vec_from_bytes(bytes: &[u8], dim: usize) -> Vec<f32> {
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

    fn backend(dim: usize, nlist: usize, nprobe: usize) -> IvfBackend {
        IvfBackend::new(
            dim,
            &IvfOptions {
                nlist,
                nprobe,
                ..Default::default()
            },
            0x9E37_79B9_7F4A_7C15,
        )
    }

    #[test]
    fn empty_backend_search_returns_nothing() {
        let b = backend(8, 4, 2);
        assert!(b.search(&[1.0; 8], 5, 0, None).unwrap().is_empty());
        assert!(b.is_empty());
    }

    #[test]
    fn finds_nearest_exact_match() {
        let mut b = backend(8, 4, 2);
        b.insert_validated(
            &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            RowId(0),
            &mut || Ok(()),
        )
        .unwrap();
        b.insert_validated(
            &[0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            RowId(1),
            &mut || Ok(()),
        )
        .unwrap();
        let top = b
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, 0, None)
            .unwrap();
        assert_eq!(top[0].0, RowId(0));
    }

    #[test]
    fn recall_at_10_against_brute_force() {
        let n = 200;
        let dim = 16;
        let mut b = backend(dim, 16, 8);
        let mut data: Vec<(Vec<f32>, RowId)> = Vec::new();
        let mut seed = 2024u64;
        for i in 0..n {
            let mut v = vec![0f32; dim];
            for x in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let u = ((seed >> 33) as u32) as f32 / (u32::MAX as f32);
                *x = u * 2.0 - 1.0;
            }
            data.push((v.clone(), RowId(i as u64)));
            b.insert_validated(&v, RowId(i as u64), &mut || Ok(()))
                .unwrap();
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
            let q = data[qi * 9 % n].0.clone();
            let truth = brute_topk(&q, 10);
            let got: std::collections::HashSet<u64> = b
                .search(&q, 10, 0, None)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.0)
                .collect();
            total_recall += truth.intersection(&got).count() as f64 / 10.0;
        }
        let avg = total_recall / queries as f64;
        assert!(avg >= 0.85, "IVF recall@10 too low: {avg:.2}");
    }

    #[test]
    fn nlist_exceeding_samples_degrades_gracefully() {
        // 5 samples, nlist=256 — must not panic.
        let mut b = backend(8, 256, 4);
        for i in 0..5u64 {
            let mut v = vec![0f32; 8];
            v[i as usize] = 1.0;
            b.insert_validated(&v, RowId(i), &mut || Ok(())).unwrap();
        }
        let top = b
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 3, 0, None)
            .unwrap();
        assert_eq!(top.len(), 3);
        assert_eq!(top[0].0, RowId(0));
    }
}
