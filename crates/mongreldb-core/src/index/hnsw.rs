//! Hierarchical Navigable Small World graph for binary-quantized vectors.
//!
//! Distance is Hamming over the quantized (1 bit/dim) vectors. The graph has a
//! hierarchy of layers; each node is assigned a top layer by a geometric
//! distribution, inserts greedily descend the upper layers and connect to the
//! `M` nearest on each visited layer, and search descends to layer 0 then runs
//! an `ef`-beam. This is the standard HNSW (Malkov & Yashunin); `recall@k` is
//! verified against brute force in the tests.

use crate::query::AiExecutionContext;
use crate::rowid::RowId;
use crate::Result;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

type Dist = u32;
/// Hard bound for graph hierarchy depth. Besides matching practical HNSW
/// implementations, this makes graph-memory admission finite and prevents a
/// pathological RNG sample from requesting an enormous layer vector.
pub(crate) const MAX_HNSW_LEVEL: i32 = 32;

/// Hamming distance (popcount of XOR) over packed-bit vectors.
fn hamming(a: &[u8], b: &[u8]) -> Dist {
    let mut d = 0u32;
    let chunks = a.len() / 8;
    let (ah, at) = a.split_at(chunks * 8);
    let (bh, bt) = b.split_at(chunks * 8);
    for (x, y) in ah.chunks_exact(8).zip(bh.chunks_exact(8)) {
        let xw = u64::from_le_bytes([x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]]);
        let yw = u64::from_le_bytes([y[0], y[1], y[2], y[3], y[4], y[5], y[6], y[7]]);
        d += (xw ^ yw).count_ones();
    }
    for (x, y) in at.iter().zip(bt.iter()) {
        d += (x ^ y).count_ones();
    }
    d
}

/// HNSW over Hamming distance on packed-bit vectors.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Hnsw {
    bytes_per_vec: usize,
    m: usize,
    ef_construction: usize,
    entry: Option<usize>,
    max_level: i32,
    vectors: Vec<Vec<u8>>,
    row_ids: Vec<RowId>,
    graph: Vec<Vec<Vec<usize>>>, // graph[node][layer] = neighbor ids
    rng_state: u64,
}

impl Hnsw {
    pub fn new(bytes_per_vec: usize, m: usize, ef_construction: usize) -> Self {
        Self {
            bytes_per_vec,
            m,
            ef_construction,
            entry: None,
            max_level: 0,
            vectors: Vec::new(),
            row_ids: Vec::new(),
            graph: Vec::new(),
            rng_state: 0x9E37_79B9_7F4A_7C15, // fixed seed for reproducibility
        }
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    pub(crate) fn options(&self) -> (usize, usize) {
        (self.m, self.ef_construction)
    }

    pub(crate) fn bytes_per_vec(&self) -> usize {
        self.bytes_per_vec
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (Vec<u8>, RowId)> + '_ {
        self.vectors
            .iter()
            .cloned()
            .zip(self.row_ids.iter().copied())
    }

    fn next_uniform(&mut self) -> f64 {
        self.rng_state = self.rng_state.wrapping_add(0x6D2B_79F5);
        let mut z = self.rng_state;
        z = (z ^ (z >> 15)).wrapping_mul(z | 1);
        z ^= z.wrapping_add((z << 7) ^ (z >> 6)).wrapping_mul(z | 61);
        ((z ^ (z >> 14)) >> 8) as f64 / ((1u64 << 56) as f64)
    }

    fn random_level(&mut self) -> i32 {
        let u = self.next_uniform();
        let ml = 1.0 / (self.m.max(2) as f64).ln();
        ((-u.ln() * ml) as i32).clamp(0, MAX_HNSW_LEVEL)
    }

    /// Insert a quantized vector bound to `row_id`.
    pub fn insert(&mut self, bits: Vec<u8>, row_id: RowId) {
        debug_assert_eq!(bits.len(), self.bytes_per_vec, "quantized length mismatch");
        let node = self.vectors.len();
        let level = self.random_level();
        self.vectors.push(bits.clone());
        self.row_ids.push(row_id);
        self.graph.push((0..=level).map(|_| Vec::new()).collect());

        if self.entry.is_none() {
            self.entry = Some(node);
            self.max_level = level;
            return;
        }
        let entry = self.entry.unwrap();
        let mut ep: Vec<(Dist, usize)> = vec![(hamming(&bits, &self.vectors[entry]), entry)];

        for lc in ((level + 1)..=self.max_level).rev() {
            ep = self.search_layer(&bits, ep, 1, lc);
        }
        for lc in (0..=level.min(self.max_level)).rev() {
            let candidates = self.search_layer(&bits, ep.clone(), self.ef_construction, lc);
            let m_layer = if lc == 0 { self.m * 2 } else { self.m };
            let mut chosen = candidates.clone();
            chosen.sort_by_key(|(d, _)| *d);
            chosen.truncate(m_layer);
            let neighbors: Vec<usize> = chosen.iter().map(|(_, n)| *n).collect();
            self.graph[node][lc as usize] = neighbors.clone();
            for &n in &neighbors {
                let adj = &mut self.graph[n][lc as usize];
                adj.push(node);
                if adj.len() > m_layer {
                    let nv = self.vectors[n].clone();
                    let neighbor_adj: Vec<usize> = adj.clone();
                    let mut scored: Vec<(Dist, usize)> = neighbor_adj
                        .iter()
                        .map(|&x| (hamming(&nv, &self.vectors[x]), x))
                        .collect();
                    scored.sort_by_key(|(d, _)| *d);
                    scored.truncate(m_layer);
                    *adj = scored.iter().map(|(_, x)| *x).collect();
                }
            }
            ep = candidates;
        }
        if level > self.max_level {
            self.max_level = level;
            self.entry = Some(node);
        }
    }

    /// k-nearest neighbors of `query_bits` (Hamming). `ef` controls the beam
    /// width (larger ⇒ higher recall).
    pub fn search(&self, query_bits: &[u8], k: usize, ef: usize) -> Vec<(RowId, Dist)> {
        self.search_with_context(query_bits, k, ef, None)
            .expect("context-free HNSW search cannot fail")
    }

    pub fn search_with_context(
        &self,
        query_bits: &[u8],
        k: usize,
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, Dist)>> {
        let Some(entry) = self.entry else {
            return Ok(Vec::new());
        };
        if let Some(context) = context {
            context.consume(crate::query::work_units(
                self.bytes_per_vec,
                crate::query::HAMMING_WORK_QUANTUM,
            ))?;
        }
        let ef = ef.max(k);
        let mut ep: Vec<(Dist, usize)> = vec![(hamming(query_bits, &self.vectors[entry]), entry)];
        for lc in (1..=self.max_level).rev() {
            ep = self.search_layer_with_context(query_bits, ep, 1, lc, context)?;
        }
        let mut results = self.search_layer_with_context(query_bits, ep, ef, 0, context)?;
        results.sort_by_key(|(d, _)| *d);
        Ok(results
            .into_iter()
            .take(k)
            .map(|(d, n)| (self.row_ids[n], d))
            .collect())
    }

    /// Greedy/beam best-first search on a single layer; returns up to `ef`
    /// nearest (dist, node) pairs.
    fn search_layer(
        &self,
        query_bits: &[u8],
        entry_points: Vec<(Dist, usize)>,
        ef: usize,
        layer: i32,
    ) -> Vec<(Dist, usize)> {
        let mut visited: HashSet<usize> = entry_points.iter().map(|(_, n)| *n).collect();
        let mut candidates: BinaryHeap<Reverse<(Dist, usize)>> = entry_points
            .iter()
            .map(|(d, n)| Reverse((*d, *n)))
            .collect();
        let mut results: BinaryHeap<(Dist, usize)> =
            entry_points.iter().map(|(d, n)| (*d, *n)).collect();
        // BinaryHeap is a max-heap; for `results` we want to pop the farthest,
        // which is the max — exactly the default behavior.

        while let Some(Reverse((cd, c))) = candidates.pop() {
            let worst = results.peek().map(|(d, _)| *d).unwrap_or(Dist::MAX);
            if cd > worst && results.len() >= ef {
                break;
            }
            for &e in &self.graph[c][layer as usize] {
                if visited.insert(e) {
                    let d = hamming(query_bits, &self.vectors[e]);
                    let w = results.peek().map(|(dd, _)| *dd).unwrap_or(Dist::MAX);
                    if d < w || results.len() < ef {
                        candidates.push(Reverse((d, e)));
                        results.push((d, e));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        results.into_vec()
    }

    fn search_layer_with_context(
        &self,
        query_bits: &[u8],
        entry_points: Vec<(Dist, usize)>,
        ef: usize,
        layer: i32,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(Dist, usize)>> {
        let mut visited: HashSet<usize> = entry_points.iter().map(|(_, n)| *n).collect();
        let mut candidates: BinaryHeap<Reverse<(Dist, usize)>> = entry_points
            .iter()
            .map(|(d, n)| Reverse((*d, *n)))
            .collect();
        let mut results: BinaryHeap<(Dist, usize)> =
            entry_points.iter().map(|(d, n)| (*d, *n)).collect();
        while let Some(Reverse((cd, c))) = candidates.pop() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let worst = results.peek().map(|(d, _)| *d).unwrap_or(Dist::MAX);
            if cd > worst && results.len() >= ef {
                break;
            }
            for &e in &self.graph[c][layer as usize] {
                if visited.insert(e) {
                    if let Some(context) = context {
                        context.consume(crate::query::work_units(
                            self.bytes_per_vec,
                            crate::query::HAMMING_WORK_QUANTUM,
                        ))?;
                    }
                    let d = hamming(query_bits, &self.vectors[e]);
                    let w = results.peek().map(|(dd, _)| *dd).unwrap_or(Dist::MAX);
                    if d < w || results.len() < ef {
                        candidates.push(Reverse((d, e)));
                        results.push((d, e));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        Ok(results.into_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_exact_match_at_distance_zero() {
        let mut h = Hnsw::new(2, 8, 32);
        h.insert(vec![0b1010_1010, 0b0000_1111], RowId(0));
        h.insert(vec![0b0101_0101, 0b1111_0000], RowId(1));
        h.insert(vec![0b1010_1010, 0b0000_1111], RowId(2));
        let top = h.search(&[0b1010_1010, 0b0000_1111], 1, 32);
        assert_eq!(top[0].1, 0); // identical ⇒ distance 0
    }

    #[test]
    fn hamming_work_scales_with_vector_width() {
        let mut narrow = Hnsw::new(1, 8, 32);
        narrow.insert(vec![0], RowId(0));
        let narrow_context = AiExecutionContext::new(None, usize::MAX);
        narrow
            .search_with_context(&[0], 1, 32, Some(&narrow_context))
            .unwrap();

        let mut wide = Hnsw::new(128, 8, 32);
        wide.insert(vec![0; 128], RowId(0));
        let wide_context = AiExecutionContext::new(None, usize::MAX);
        wide.search_with_context(&[0; 128], 1, 32, Some(&wide_context))
            .unwrap();

        assert!(wide_context.consumed_work() > narrow_context.consumed_work());
    }

    #[test]
    fn recall_against_brute_force_on_random_data() {
        let n = 300;
        let bpv = 16;
        let mut data: Vec<(Vec<u8>, RowId)> = Vec::with_capacity(n);
        let mut seed = 12345u64;
        for i in 0..n {
            let mut v = vec![0u8; bpv];
            for b in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *b = (seed >> 33) as u8;
            }
            data.push((v, RowId(i as u64)));
        }
        let mut h = Hnsw::new(bpv, 16, 64);
        for (v, rid) in &data {
            h.insert(v.clone(), *rid);
        }

        let brute_topk = |q: &[u8], k: usize| -> std::collections::HashSet<u64> {
            let mut s: Vec<(u32, u64)> =
                data.iter().map(|(v, rid)| (hamming(q, v), rid.0)).collect();
            s.sort_by_key(|(d, _)| *d);
            s.into_iter().take(k).map(|(_, r)| r).collect()
        };

        let mut total_recall = 0.0;
        let queries = 20;
        for qi in 0..queries {
            let q = data[qi * 7 % n].0.clone();
            let truth = brute_topk(&q, 10);
            let got: std::collections::HashSet<u64> =
                h.search(&q, 10, 64).into_iter().map(|(r, _)| r.0).collect();
            let inter = truth.intersection(&got).count() as f64;
            total_recall += inter / 10.0;
        }
        let avg = total_recall / queries as f64;
        assert!(avg >= 0.90, "HNSW recall@10 too low: {avg:.2}");
    }
}

/// Cosine distance `1 - cosine_similarity`. Either zero-norm side yields
/// similarity 0 and distance 1. Callers must validate finite values and equal
/// dimensions before invoking.
pub(crate) fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let norm_a = norm_a.sqrt();
    let norm_b = norm_b.sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 1.0;
    }
    1.0 - (dot / (norm_a * norm_b))
}

/// Total-order wrapper so `BinaryHeap` can rank cosine distances.
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

/// HNSW over cosine distance on full-precision f32 vectors.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DenseHnsw {
    dim: usize,
    m: usize,
    ef_construction: usize,
    entry: Option<usize>,
    max_level: i32,
    vectors: Vec<Vec<f32>>,
    row_ids: Vec<RowId>,
    graph: Vec<Vec<Vec<usize>>>, // graph[node][layer] = neighbor ids
    rng_state: u64,
}

impl DenseHnsw {
    pub fn new(dim: usize, m: usize, ef_construction: usize) -> Self {
        Self {
            dim,
            m,
            ef_construction,
            entry: None,
            max_level: 0,
            vectors: Vec::new(),
            row_ids: Vec::new(),
            graph: Vec::new(),
            rng_state: 0x9E37_79B9_7F4A_7C15, // fixed seed for reproducibility
        }
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    pub(crate) fn options(&self) -> (usize, usize) {
        (self.m, self.ef_construction)
    }

    pub(crate) fn dim(&self) -> usize {
        self.dim
    }

    pub(crate) fn entries(&self) -> impl Iterator<Item = (Vec<f32>, RowId)> + '_ {
        self.vectors
            .iter()
            .cloned()
            .zip(self.row_ids.iter().copied())
    }

    fn next_uniform(&mut self) -> f64 {
        self.rng_state = self.rng_state.wrapping_add(0x6D2B_79F5);
        let mut z = self.rng_state;
        z = (z ^ (z >> 15)).wrapping_mul(z | 1);
        z ^= z.wrapping_add((z << 7) ^ (z >> 6)).wrapping_mul(z | 61);
        ((z ^ (z >> 14)) >> 8) as f64 / ((1u64 << 56) as f64)
    }

    fn random_level(&mut self) -> i32 {
        let u = self.next_uniform();
        let ml = 1.0 / (self.m.max(2) as f64).ln();
        ((-u.ln() * ml) as i32).clamp(0, MAX_HNSW_LEVEL)
    }

    /// Insert a full-precision vector bound to `row_id`.
    pub fn insert(&mut self, vec: Vec<f32>, row_id: RowId) {
        self.insert_with_checkpoint(vec, row_id, || Ok(()))
            .expect("unlimited Dense HNSW insertion cannot fail");
    }

    /// Insert with cooperative checks inside graph traversal and rewiring.
    pub(crate) fn insert_with_checkpoint<F>(
        &mut self,
        vec: Vec<f32>,
        row_id: RowId,
        mut checkpoint: F,
    ) -> Result<()>
    where
        F: FnMut() -> Result<()>,
    {
        debug_assert_eq!(vec.len(), self.dim, "dense vector length mismatch");
        checkpoint()?;
        let node = self.vectors.len();
        let level = self.random_level();
        self.vectors.push(vec.clone());
        self.row_ids.push(row_id);
        self.graph.push((0..=level).map(|_| Vec::new()).collect());

        if self.entry.is_none() {
            self.entry = Some(node);
            self.max_level = level;
            return Ok(());
        }
        let entry = self.entry.unwrap();
        let mut ep: Vec<(f32, usize)> = vec![(cosine_distance(&vec, &self.vectors[entry]), entry)];

        for lc in ((level + 1)..=self.max_level).rev() {
            ep = self.search_layer_with_checkpoint(&vec, ep, 1, lc, &mut checkpoint)?;
        }
        for lc in (0..=level.min(self.max_level)).rev() {
            let candidates = self.search_layer_with_checkpoint(
                &vec,
                ep.clone(),
                self.ef_construction,
                lc,
                &mut checkpoint,
            )?;
            let m_layer = if lc == 0 { self.m * 2 } else { self.m };
            let mut chosen = candidates.clone();
            chosen.sort_by(|(da, _), (db, _)| da.total_cmp(db));
            chosen.truncate(m_layer);
            let neighbors: Vec<usize> = chosen.iter().map(|(_, n)| *n).collect();
            self.graph[node][lc as usize] = neighbors.clone();
            for &n in &neighbors {
                checkpoint()?;
                let adj = &mut self.graph[n][lc as usize];
                adj.push(node);
                if adj.len() > m_layer {
                    let nv = self.vectors[n].clone();
                    let neighbor_adj: Vec<usize> = adj.clone();
                    let mut scored: Vec<(f32, usize)> = neighbor_adj
                        .iter()
                        .map(|&x| (cosine_distance(&nv, &self.vectors[x]), x))
                        .collect();
                    scored.sort_by(|(da, _), (db, _)| da.total_cmp(db));
                    scored.truncate(m_layer);
                    *adj = scored.iter().map(|(_, x)| *x).collect();
                }
            }
            ep = candidates;
        }
        if level > self.max_level {
            self.max_level = level;
            self.entry = Some(node);
        }
        Ok(())
    }

    /// k-nearest neighbors of `query` (cosine distance). `ef` controls beam
    /// width (larger ⇒ higher recall).
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(RowId, f32)> {
        self.search_with_context(query, k, ef, None)
            .expect("context-free dense HNSW search cannot fail")
    }

    pub fn search_with_context(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(RowId, f32)>> {
        let Some(entry) = self.entry else {
            return Ok(Vec::new());
        };
        if let Some(context) = context {
            context.consume(crate::query::work_units(
                self.dim,
                crate::query::FLOAT_WORK_QUANTUM,
            ))?;
        }
        let ef = ef.max(k);
        let mut ep: Vec<(f32, usize)> = vec![(cosine_distance(query, &self.vectors[entry]), entry)];
        for lc in (1..=self.max_level).rev() {
            ep = self.search_layer_with_context(query, ep, 1, lc, context)?;
        }
        let mut results = self.search_layer_with_context(query, ep, ef, 0, context)?;
        results.sort_by(|(da, _), (db, _)| da.total_cmp(db));
        Ok(results
            .into_iter()
            .take(k)
            .map(|(d, n)| (self.row_ids[n], d))
            .collect())
    }

    fn search_layer_with_checkpoint<F>(
        &self,
        query: &[f32],
        entry_points: Vec<(f32, usize)>,
        ef: usize,
        layer: i32,
        checkpoint: &mut F,
    ) -> Result<Vec<(f32, usize)>>
    where
        F: FnMut() -> Result<()>,
    {
        let mut visited: HashSet<usize> = entry_points.iter().map(|(_, n)| *n).collect();
        let mut candidates: BinaryHeap<Reverse<(DistF32, usize)>> = entry_points
            .iter()
            .map(|(distance, node)| Reverse((DistF32(*distance), *node)))
            .collect();
        let mut results: BinaryHeap<(DistF32, usize)> = entry_points
            .iter()
            .map(|(distance, node)| (DistF32(*distance), *node))
            .collect();

        while let Some(Reverse((candidate_distance, candidate))) = candidates.pop() {
            checkpoint()?;
            let worst = results
                .peek()
                .map(|(distance, _)| distance.0)
                .unwrap_or(f32::INFINITY);
            if candidate_distance.0 > worst && results.len() >= ef {
                break;
            }
            for &neighbor in &self.graph[candidate][layer as usize] {
                checkpoint()?;
                if visited.insert(neighbor) {
                    let distance = cosine_distance(query, &self.vectors[neighbor]);
                    let worst = results
                        .peek()
                        .map(|(distance, _)| distance.0)
                        .unwrap_or(f32::INFINITY);
                    if distance < worst || results.len() < ef {
                        candidates.push(Reverse((DistF32(distance), neighbor)));
                        results.push((DistF32(distance), neighbor));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        Ok(results
            .into_iter()
            .map(|(distance, node)| (distance.0, node))
            .collect())
    }

    fn search_layer_with_context(
        &self,
        query: &[f32],
        entry_points: Vec<(f32, usize)>,
        ef: usize,
        layer: i32,
        context: Option<&AiExecutionContext>,
    ) -> Result<Vec<(f32, usize)>> {
        let mut visited: HashSet<usize> = entry_points.iter().map(|(_, n)| *n).collect();
        let mut candidates: BinaryHeap<Reverse<(DistF32, usize)>> = entry_points
            .iter()
            .map(|(d, n)| Reverse((DistF32(*d), *n)))
            .collect();
        let mut results: BinaryHeap<(DistF32, usize)> = entry_points
            .iter()
            .map(|(d, n)| (DistF32(*d), *n))
            .collect();
        while let Some(Reverse((cd, c))) = candidates.pop() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let worst = results.peek().map(|(d, _)| d.0).unwrap_or(f32::INFINITY);
            if cd.0 > worst && results.len() >= ef {
                break;
            }
            for &e in &self.graph[c][layer as usize] {
                if visited.insert(e) {
                    if let Some(context) = context {
                        context.consume(crate::query::work_units(
                            self.dim,
                            crate::query::FLOAT_WORK_QUANTUM,
                        ))?;
                    }
                    let d = cosine_distance(query, &self.vectors[e]);
                    let w = results.peek().map(|(dd, _)| dd.0).unwrap_or(f32::INFINITY);
                    if d < w || results.len() < ef {
                        candidates.push(Reverse((DistF32(d), e)));
                        results.push((DistF32(d), e));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        Ok(results.into_iter().map(|(d, n)| (d.0, n)).collect())
    }
}

#[cfg(test)]
mod dense_tests {
    use super::*;

    #[test]
    fn cosine_zero_norm_is_distance_one() {
        assert_eq!(cosine_distance(&[0.0, 0.0], &[1.0, 0.0]), 1.0);
        assert_eq!(cosine_distance(&[1.0, 0.0], &[0.0, 0.0]), 1.0);
        assert_eq!(cosine_distance(&[0.0, 0.0], &[0.0, 0.0]), 1.0);
    }

    #[test]
    fn cosine_identical_is_distance_zero() {
        let v = [0.5f32, -0.25, 1.0];
        let d = cosine_distance(&v, &v);
        assert!(d.abs() < 1e-6, "identical cosine distance {d}");
    }

    #[test]
    fn dense_finds_exact_match() {
        let mut h = DenseHnsw::new(3, 8, 32);
        h.insert(vec![1.0, 0.0, 0.0], RowId(0));
        h.insert(vec![0.0, 1.0, 0.0], RowId(1));
        h.insert(vec![1.0, 0.0, 0.0], RowId(2));
        let top = h.search(&[1.0, 0.0, 0.0], 1, 32);
        assert_eq!(top[0].1, 0.0);
        assert!(top[0].0 == RowId(0) || top[0].0 == RowId(2));
    }

    #[test]
    fn dense_float_work_scales_with_dim() {
        let mut narrow = DenseHnsw::new(4, 8, 32);
        narrow.insert(vec![1.0; 4], RowId(0));
        let narrow_context = AiExecutionContext::new(None, usize::MAX);
        narrow
            .search_with_context(&[1.0; 4], 1, 32, Some(&narrow_context))
            .unwrap();

        let mut wide = DenseHnsw::new(512, 8, 32);
        wide.insert(vec![1.0; 512], RowId(0));
        let wide_context = AiExecutionContext::new(None, usize::MAX);
        wide.search_with_context(&[1.0; 512], 1, 32, Some(&wide_context))
            .unwrap();

        assert!(wide_context.consumed_work() > narrow_context.consumed_work());
    }

    #[test]
    fn dense_recall_against_brute_force() {
        let n = 300;
        let dim = 32;
        let mut data: Vec<(Vec<f32>, RowId)> = Vec::with_capacity(n);
        let mut seed = 12345u64;
        for i in 0..n {
            let mut v = vec![0f32; dim];
            for b in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                // Signed random in [-1, 1)
                let u = ((seed >> 33) as u32) as f32 / (u32::MAX as f32);
                *b = u * 2.0 - 1.0;
            }
            data.push((v, RowId(i as u64)));
        }
        let mut h = DenseHnsw::new(dim, 16, 64);
        for (v, rid) in &data {
            h.insert(v.clone(), *rid);
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
            let got: std::collections::HashSet<u64> =
                h.search(&q, 10, 64).into_iter().map(|(r, _)| r.0).collect();
            let inter = truth.intersection(&got).count() as f64;
            total_recall += inter / 10.0;
        }
        let avg = total_recall / queries as f64;
        assert!(avg >= 0.90, "dense HNSW recall@10 too low: {avg:.2}");
    }
}
