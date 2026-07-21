//! DiskANN (Vamana) backend.
//!
//! Single-layer robust-pruned graph with bounded degree `R`. Unlike HNSW's
//! layered structure, Vamana builds one flat graph: every node is connected to
//! its robust-pruned `R` nearest neighbors, with an `alpha` distance threshold
//! that favors diversity (a candidate is pruned if it is closer to an already
//! selected neighbor than to the query by factor `alpha`).
//!
//! Search is a greedy beam walk from a fixed entry point. The `beam_width`
//! parameter floors the search candidate-list size (ensuring at least that
//! many candidates are considered per expansion round); in our embedded model
//! there is no separate on-disk vector file, so `beam_width` does not bound
//! SSD I/O as in the original DiskANN paper — it is a search-quality floor.
//! Search is bounded by the work-budget/deadline via [`AiExecutionContext`]
//! (checkpoint + work-consume per candidate expansion). The build path is
//! currently uninterruptible within a single vector insertion (coarse
//! per-row cancellation only); a future enhancement may thread the context
//! through the build search calls.
//!
//! ## Determinism
//!
//! The build is deterministic: a fixed seed picks the entry point and the
//! insertion order is the entry order; robust-prune tie-breaks by node id, so
//! the same vectors in the same order produce a byte-identical graph. This
//! preserves the consolidation guarantee (rebuilt base matches base-only).
//!
//! ## Representation
//!
//! Dense vectors are stored full-precision (cosine distance). The "disk" in
//! DiskANN historically refers to memory-mapped vector pages; in our embedded
//! model the graph + vectors are in-memory and bounded by the memory governor
//! reservation, with no separate on-disk vector file.

use crate::index::ann::backend::{AnnBackend, AnnBackendCheckpoint, BackendMetric};
use crate::index::hnsw::cosine_distance;
use crate::query::AiExecutionContext;
use crate::rowid::RowId;
use crate::schema::DiskAnnOptions;
use crate::Result;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

pub(crate) const SEED: u64 = 0x9E37_79B9_7F4A_7C15;

/// One DiskANN (Vamana) backend: a single-layer graph over full-precision
/// Dense vectors.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct DiskAnnBackend {
    dim: usize,
    r: usize,
    l: usize,
    beam_width: usize,
    /// alpha × 100 (120 = 1.2).
    alpha: u32,
    vectors: Vec<Vec<f32>>,
    row_ids: Vec<RowId>,
    /// `graph[node]` = neighbor node ids (bounded by `r`).
    graph: Vec<Vec<usize>>,
    entry: Option<usize>,
    /// Fixed seed for deterministic entry-point selection.
    seed: u64,
}

impl DiskAnnBackend {
    pub(crate) fn new(dim: usize, options: &DiskAnnOptions, seed: u64) -> Self {
        Self {
            dim,
            r: options.r,
            l: options.l,
            beam_width: options.beam_width,
            alpha: options.alpha,
            vectors: Vec::new(),
            row_ids: Vec::new(),
            graph: Vec::new(),
            entry: None,
            seed,
        }
    }

    pub(crate) fn matches_checkpoint(&self, dim: usize, options: &DiskAnnOptions) -> bool {
        self.dim == dim
            && (self.r, self.l, self.beam_width, self.alpha)
                == (options.r, options.l, options.beam_width, options.alpha)
            && self.vectors.len() == self.row_ids.len()
            && self.vectors.len() == self.graph.len()
            && self.entry.is_some() == !self.vectors.is_empty()
            && self.entry.is_none_or(|entry| entry < self.vectors.len())
            && self.seed == SEED
            && self
                .vectors
                .iter()
                .all(|vector| vector.len() == dim && vector.iter().all(|v| v.is_finite()))
            && self.graph.iter().enumerate().all(|(node, neighbors)| {
                let mut unique = HashSet::with_capacity(neighbors.len());
                neighbors.len() <= self.r
                    && neighbors.iter().all(|neighbor| {
                        *neighbor < self.vectors.len()
                            && *neighbor != node
                            && unique.insert(*neighbor)
                    })
            })
    }

    fn distance(&self, a: usize, query: &[f32]) -> f32 {
        cosine_distance(query, &self.vectors[a])
    }

    /// Greedy beam search from `entry` returning up to `ef` (node, distance)
    /// pairs sorted ascending. This is the Vamana search routine.
    fn search_layer(
        &self,
        query: &[f32],
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(f32, usize)>> {
        let Some(entry) = self.entry else {
            return Ok(Vec::new());
        };
        let mut visited: HashSet<usize> = HashSet::new();
        visited.insert(entry);
        let entry_dist = self.distance(entry, query);
        // Min-heap of (dist, node) candidates to expand.
        let mut candidates: BinaryHeap<Reverse<(DistF32, usize)>> =
            BinaryHeap::from([Reverse((DistF32(entry_dist), entry))]);
        // Max-heap keeps the worst distance and highest RowId at the top.
        let mut results: BinaryHeap<(DistF32, RowId, usize)> =
            BinaryHeap::from([(DistF32(entry_dist), self.row_ids[entry], entry)]);
        while let Some(Reverse((cd, c))) = candidates.pop() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let worst = results.peek().map(|(d, _, _)| d.0).unwrap_or(f32::INFINITY);
            if cd.0 > worst && results.len() >= ef {
                break;
            }
            for &neighbor in &self.graph[c] {
                if visited.insert(neighbor) {
                    if let Some(context) = context {
                        context.consume(crate::query::work_units(
                            self.dim,
                            crate::query::FLOAT_WORK_QUANTUM,
                        ))?;
                    }
                    let d = self.distance(neighbor, query);
                    let worst = results
                        .peek()
                        .map(|(distance, row_id, _)| (*distance, *row_id))
                        .unwrap_or((DistF32(f32::INFINITY), RowId(u64::MAX)));
                    if (DistF32(d), self.row_ids[neighbor]) < worst || results.len() < ef {
                        candidates.push(Reverse((DistF32(d), neighbor)));
                        results.push((DistF32(d), self.row_ids[neighbor], neighbor));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        let mut out: Vec<(f32, usize)> = results
            .into_iter()
            .map(|(distance, _, node)| (distance.0, node))
            .collect();
        out.sort_by(|(da, na), (db, nb)| {
            da.total_cmp(db)
                .then_with(|| self.row_ids[*na].cmp(&self.row_ids[*nb]))
        });
        Ok(out)
    }

    /// Robust prune (Vamana): from `candidates` (already carrying their
    /// distance to the inserting node) select up to `r` neighbors, preferring
    /// diverse directions. A candidate `p` is pruned if some already-selected
    /// `p'` satisfies `dist(p', p) * alpha < dist(p, node)`. Ties break by
    /// node id for determinism.
    fn robust_prune(&self, candidates: &mut Vec<(f32, usize)>) -> Vec<usize> {
        robust_prune_owned(&self.vectors, candidates, self.r, self.alpha)
    }

    /// Insert a vector and wire it into the graph via Vamana robust prune.
    fn insert_vec(&mut self, vec: Vec<f32>, row_id: RowId) {
        let node = self.vectors.len();
        self.vectors.push(vec);
        self.row_ids.push(row_id);
        self.graph.push(Vec::new());
        if self.entry.is_none() {
            self.entry = Some(node);
            return;
        }
        // Search for candidate neighbors with build beam `L`. Clone the node's
        // vector to avoid borrowing self across the (immutable) search call.
        let query = self.vectors[node].clone();
        let ef = self.l.max(self.r);
        let mut candidates = self.search_layer(&query, ef, None).unwrap_or_default();
        let neighbors = self.robust_prune(&mut candidates);
        // Bidirectional connect, capping degree at R on both sides.
        self.graph[node] = neighbors.clone();
        for &n in &neighbors {
            // Snapshot this node's adjacency + vector so we can re-prune
            // without holding a mutable borrow of self.graph across the
            // (immutable) distance reads.
            let over_capacity = self.graph[n].len() >= self.r;
            if !over_capacity {
                self.graph[n].push(node);
                continue;
            }
            let nv = self.vectors[n].clone();
            let mut adj_candidates: Vec<(f32, usize)> = self.graph[n]
                .iter()
                .map(|&x| (cosine_distance(&nv, &self.vectors[x]), x))
                .collect();
            adj_candidates.push((cosine_distance(&nv, &self.vectors[node]), node));
            let r = self.r;
            let alpha = self.alpha;
            let pruned = robust_prune_owned(&self.vectors, &mut adj_candidates, r, alpha);
            self.graph[n] = pruned;
        }
    }
}

/// Free-function robust prune over an external vectors slice. Used when the
/// caller already holds a mutable borrow of `self.graph` and cannot call the
/// `&self` method. `candidates` carry their distance to the node being pruned
/// (so no query vector is needed).
fn robust_prune_owned(
    vectors: &[Vec<f32>],
    candidates: &mut Vec<(f32, usize)>,
    r: usize,
    alpha100: u32,
) -> Vec<usize> {
    candidates.sort_by(|(da, na), (db, nb)| da.total_cmp(db).then_with(|| na.cmp(nb)));
    candidates.dedup_by(|a, b| a.1 == b.1);
    let alpha = alpha100 as f32 / 100.0;
    let mut selected: Vec<usize> = Vec::with_capacity(r);
    let mut i = 0;
    while i < candidates.len() && selected.len() < r {
        let (p_dist, p) = candidates[i];
        let mut pruned = false;
        for &s in &selected {
            let d_sp = cosine_distance(&vectors[p], &vectors[s]);
            if d_sp * alpha < p_dist {
                pruned = true;
                break;
            }
        }
        if !pruned {
            selected.push(p);
        }
        i += 1;
    }
    selected
}

/// Total-order f32 wrapper for BinaryHeap ranking (NaN-safe).
#[derive(Clone, Copy, Debug)]
struct DistF32(f32);

impl PartialEq for DistF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.total_cmp(&other.0) == std::cmp::Ordering::Equal
    }
}
impl Eq for DistF32 {}
impl PartialOrd for DistF32 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for DistF32 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl AnnBackend for DiskAnnBackend {
    fn metric(&self) -> BackendMetric {
        BackendMetric::Cosine
    }

    fn len(&self) -> usize {
        self.vectors.len()
    }

    fn insert_validated(
        &mut self,
        vec: &[f32],
        row_id: RowId,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        checkpoint()?;
        self.insert_vec(vec.to_vec(), row_id);
        Ok(())
    }

    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, f64)>> {
        let ef = ef.max(k).max(self.beam_width);
        let results = self.search_layer(query, ef, context)?;
        Ok(results
            .into_iter()
            .take(k)
            .map(|(dist, node)| (self.row_ids[node], f64::from(dist)))
            .collect())
    }

    fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        self.vectors
            .iter()
            .zip(self.row_ids.iter())
            .map(|(vec, row_id)| {
                let mut bytes = Vec::with_capacity(vec.len() * 4);
                for value in vec {
                    bytes.extend_from_slice(&value.to_le_bytes());
                }
                (bytes, *row_id)
            })
            .collect()
    }

    fn freeze(&self) -> AnnBackendCheckpoint {
        AnnBackendCheckpoint::DiskAnn {
            dim: self.dim,
            r: self.r,
            l: self.l,
            beam_width: self.beam_width,
            alpha: self.alpha,
            graph: self.clone(),
        }
    }

    fn empty_active(&self) -> Box<dyn AnnBackend> {
        Box::new(Self::new(
            self.dim,
            &DiskAnnOptions {
                r: self.r,
                l: self.l,
                beam_width: self.beam_width,
                alpha: self.alpha,
            },
            self.seed,
        ))
    }

    fn rebuild_from_entries(&self, entries: &[(Vec<u8>, RowId)]) -> Box<dyn AnnBackend> {
        let mut backend = Self::new(
            self.dim,
            &DiskAnnOptions {
                r: self.r,
                l: self.l,
                beam_width: self.beam_width,
                alpha: self.alpha,
            },
            self.seed,
        );
        for (bytes, row_id) in entries {
            let vec = le_bytes_to_vec(bytes, self.dim);
            backend.insert_vec(vec, *row_id);
        }
        Box::new(backend)
    }

    fn clone_box(&self) -> Box<dyn AnnBackend> {
        Box::new(self.clone())
    }
}

fn le_bytes_to_vec(bytes: &[u8], dim: usize) -> Vec<f32> {
    debug_assert_eq!(bytes.len(), dim * 4, "diskann entry size mismatch");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(dim: usize) -> DiskAnnBackend {
        DiskAnnBackend::new(
            dim,
            &DiskAnnOptions {
                r: 16,
                l: 32,
                beam_width: 4,
                alpha: 120,
            },
            0x9E37_79B9_7F4A_7C15,
        )
    }

    #[test]
    fn empty_backend_search_returns_nothing() {
        let b = backend(8);
        assert!(b.search(&[1.0; 8], 5, 16, None).unwrap().is_empty());
        assert!(b.is_empty());
    }

    #[test]
    fn finds_nearest_exact_match() {
        let mut b = backend(8);
        b.insert_vec(vec![1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], RowId(0));
        b.insert_vec(vec![0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], RowId(1));
        b.insert_vec(vec![0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0], RowId(2));
        let top = b
            .search(&[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], 1, 16, None)
            .unwrap();
        assert_eq!(top[0].0, RowId(0));
    }

    #[test]
    fn equal_distances_break_ties_by_row_id_before_top_k() {
        let mut b = backend(8);
        b.insert_vec(vec![1.0; 8], RowId(100));
        b.insert_vec(vec![1.0; 8], RowId(1));
        let top = b.search(&[1.0; 8], 1, 16, None).unwrap();
        assert_eq!(top[0].0, RowId(1));
    }

    #[test]
    fn insertion_never_exceeds_degree_bound() {
        let mut b = backend(8);
        for id in 0..100u64 {
            let mut vector = vec![0.0; 8];
            vector[id as usize % 8] = 1.0;
            vector[(id as usize + 3) % 8] = id as f32 / 100.0;
            b.insert_vec(vector, RowId(id));
        }
        assert!(b.graph.iter().all(|neighbors| neighbors.len() <= b.r));
    }

    #[test]
    fn malformed_checkpoint_graph_is_rejected() {
        let options = DiskAnnOptions {
            r: 16,
            l: 32,
            beam_width: 4,
            alpha: 120,
        };
        let mut b = backend(8);
        b.insert_vec(vec![1.0; 8], RowId(1));
        b.entry = None;
        assert!(!b.matches_checkpoint(8, &options));

        b.entry = Some(0);
        b.graph[0] = vec![0];
        assert!(!b.matches_checkpoint(8, &options));
    }

    #[test]
    fn recall_at_10_against_brute_force() {
        let n = 200;
        let dim = 16;
        let mut b = backend(dim);
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
            b.insert_vec(v, RowId(i as u64));
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
                .search(&q, 10, 32, None)
                .unwrap()
                .into_iter()
                .map(|(r, _)| r.0)
                .collect();
            total_recall += truth.intersection(&got).count() as f64 / 10.0;
        }
        let avg = total_recall / queries as f64;
        assert!(avg >= 0.85, "DiskANN recall@10 too low: {avg:.2}");
    }

    #[test]
    fn build_is_deterministic() {
        let dim = 8;
        let mut data: Vec<(Vec<f32>, RowId)> = Vec::new();
        let mut seed = 77u64;
        for i in 0..30u64 {
            let mut v = vec![0f32; dim];
            for x in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *x = ((seed >> 40) as i32 as f32) / (i32::MAX as f32);
            }
            data.push((v, RowId(i)));
        }
        let mut b1 = backend(dim);
        let mut b2 = backend(dim);
        for (v, rid) in &data {
            b1.insert_vec(v.clone(), *rid);
            b2.insert_vec(v.clone(), *rid);
        }
        assert_eq!(
            b1.graph, b2.graph,
            "same data + seed must yield identical graph"
        );
        assert_eq!(b1.entry, b2.entry);
    }

    #[test]
    fn deadline_cancels_search() {
        let mut b = backend(32);
        for i in 0..200u64 {
            let mut v = vec![0f32; 32];
            v[(i as usize) % 32] = 1.0;
            b.insert_vec(v, RowId(i));
        }
        let deadline = Some(std::time::Instant::now() - std::time::Duration::from_millis(1));
        let ctx = AiExecutionContext::new(deadline, usize::MAX);
        let err = b.search(&[1.0; 32], 10, 32, Some(&ctx)).unwrap_err();
        assert!(matches!(
            err,
            crate::MongrelError::Cancelled
                | crate::MongrelError::DeadlineExceeded
                | crate::MongrelError::WorkBudgetExceeded
        ));
    }
}
