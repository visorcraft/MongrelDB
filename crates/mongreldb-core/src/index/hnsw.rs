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
            // Bridge owner = closest selected neighbor. If no old neighbor
            // keeps the new node after independent degree-bound pruning, the
            // bridge owner is forced to retain it (degree bound preserved by
            // replacing the farthest retained edge if needed). This keeps
            // every newly inserted node reachable from the entry point on
            // every layer, including the mandatory layer-0 search graph.
            let bridge = neighbors.first().copied();
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
                    let evicted: Vec<usize> =
                        scored.iter().skip(m_layer).map(|(_, x)| *x).collect();
                    *adj = scored.iter().take(m_layer).map(|(_, x)| *x).collect();
                    // Cascading connectivity preservation (spec §7.4): if an
                    // evicted old node would lose its only incoming edge at
                    // this layer, re-establish a back-edge by adding it to
                    // the new node's adjacency. The new node's adj may grow
                    // by one entry; we truncate it below.
                    for &e in &evicted {
                        if e == node {
                            continue;
                        }
                        if !self.has_incoming_edge(e, lc as usize) {
                            self.preserve_into_new_node_adj(node, e, lc as usize, m_layer);
                        }
                    }
                }
            }
            // Bridge-owner connectivity invariant (spec §7.4).
            let still_linked = neighbors
                .iter()
                .any(|&n| self.graph[n][lc as usize].contains(&node));
            if !still_linked {
                if let Some(bridge) = bridge {
                    let force_evicted =
                        self.compute_force_eviction_hamming(bridge, node, lc as usize, m_layer);
                    self.force_bounded_edge_hamming(bridge, node, lc as usize, m_layer);
                    if let Some(e) = force_evicted {
                        if e != node && !self.has_incoming_edge(e, lc as usize) {
                            self.preserve_into_new_node_adj(node, e, lc as usize, m_layer);
                        }
                    }
                }
            }
            ep = candidates;
        }
        if level > self.max_level {
            self.max_level = level;
            self.entry = Some(node);
        }
    }

    /// Force the bridge owner to retain `new_node` at `layer` while preserving
    /// the `degree_limit` cap. If the adjacency is full, replace the farthest
    /// retained edge (in Hamming distance) with `new_node`. No-op if the
    /// owner already retains `new_node`.
    fn force_bounded_edge_hamming(
        &mut self,
        owner: usize,
        new_node: usize,
        layer: usize,
        degree_limit: usize,
    ) {
        let adj = &mut self.graph[owner][layer];
        if adj.contains(&new_node) {
            return;
        }
        if adj.len() < degree_limit {
            adj.push(new_node);
            return;
        }
        // Find the farthest retained edge; if `new_node` is not strictly
        // farther than it, replace. (Strictly farther keeps the eviction
        // meaningful; otherwise we'd swap for no benefit.)
        let owner_vec = self.vectors[owner].clone();
        let mut worst_pos: usize = 0;
        let mut worst_d: Dist = 0;
        for (pos, &x) in adj.iter().enumerate() {
            let d = hamming(&owner_vec, &self.vectors[x]);
            if d >= worst_d {
                worst_d = d;
                worst_pos = pos;
            }
        }
        let new_d = hamming(&owner_vec, &self.vectors[new_node]);
        if new_d < worst_d {
            adj[worst_pos] = new_node;
        } else {
            // `new_node` is the farthest — evict the previous worst to keep the
            // invariant that the new node is reachable from the bridge owner.
            adj[worst_pos] = new_node;
        }
    }

    /// Compute (without mutating) which entry would be evicted by
    /// [`force_bounded_edge_hamming`](Self::force_bounded_edge_hamming). Returns
    /// `None` if the force would not evict anything (adjacency already
    /// contains `new_node`, or has spare capacity).
    fn compute_force_eviction_hamming(
        &self,
        owner: usize,
        new_node: usize,
        layer: usize,
        degree_limit: usize,
    ) -> Option<usize> {
        let adj = &self.graph[owner][layer];
        if adj.contains(&new_node) {
            return None;
        }
        if adj.len() < degree_limit {
            return None;
        }
        let owner_vec = &self.vectors[owner];
        let mut worst_pos: usize = 0;
        let mut worst_d: Dist = 0;
        for (pos, &x) in adj.iter().enumerate() {
            let d = hamming(owner_vec, &self.vectors[x]);
            if d >= worst_d {
                worst_d = d;
                worst_pos = pos;
            }
        }
        Some(adj[worst_pos])
    }

    /// Whether `node` has at least one incoming edge at `layer` (any other
    /// node has it in their `graph[...][layer]` adjacency list). Used by
    /// the cascading connectivity preservation step.
    fn has_incoming_edge(&self, node: usize, layer: usize) -> bool {
        if layer > MAX_HNSW_LEVEL as usize {
            return false;
        }
        for adj in &self.graph {
            if layer < adj.len() && adj[layer].contains(&node) {
                return true;
            }
        }
        false
    }

    /// Re-establish a back-edge from the new node to `evicted_node` by
    /// adding `evicted_node` to `new_node`'s adjacency at `layer`. The
    /// degree bound is enforced: if full, the farthest edge (in Hamming
    /// distance from `new_node`) is replaced — preserving the invariant
    /// that `evicted_node` remains reachable from the entry point.
    fn preserve_into_new_node_adj(
        &mut self,
        new_node: usize,
        evicted_node: usize,
        layer: usize,
        degree_limit: usize,
    ) {
        let adj = &mut self.graph[new_node][layer];
        if adj.contains(&evicted_node) {
            return;
        }
        if adj.len() < degree_limit {
            adj.push(evicted_node);
            return;
        }
        // Adjacency is full — replace the farthest retained edge.
        let nv = self.vectors[new_node].clone();
        let mut worst_pos: usize = 0;
        let mut worst_d: Dist = 0;
        for (pos, &x) in adj.iter().enumerate() {
            let d = hamming(&nv, &self.vectors[x]);
            if d >= worst_d {
                worst_d = d;
                worst_pos = pos;
            }
        }
        // Always replace — the connectivity invariant requires the evicted
        // node to remain reachable from the new node regardless of distance
        // (spec §7.4).
        adj[worst_pos] = evicted_node;
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
        results.sort_by_key(|(distance, node)| (*distance, self.row_ids[*node]));
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
        let mut results: BinaryHeap<(Dist, RowId, usize)> = entry_points
            .iter()
            .map(|(d, n)| (*d, self.row_ids[*n], *n))
            .collect();
        // BinaryHeap is a max-heap; for `results` we want to pop the farthest,
        // which is the max — exactly the default behavior.

        while let Some(Reverse((cd, c))) = candidates.pop() {
            let worst = results.peek().map(|(d, _, _)| *d).unwrap_or(Dist::MAX);
            if cd > worst && results.len() >= ef {
                break;
            }
            for &e in &self.graph[c][layer as usize] {
                if visited.insert(e) {
                    let d = hamming(query_bits, &self.vectors[e]);
                    // Always queue the neighbor for further exploration —
                    // even when the result heap is full. The strict-key
                    // admission check below would otherwise stop the beam at
                    // row_ids larger than the worst in `W` and hide strictly
                    // closer nodes reachable through equal-distance hops.
                    candidates.push(Reverse((d, e)));
                    let worst_key = results
                        .peek()
                        .map(|(distance, row_id, _)| (*distance, *row_id))
                        .unwrap_or((Dist::MAX, RowId(u64::MAX)));
                    let key = (d, self.row_ids[e]);
                    if key < worst_key || results.len() < ef {
                        results.push((d, self.row_ids[e], e));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        results
            .into_vec()
            .into_iter()
            .map(|(distance, _, node)| (distance, node))
            .collect()
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
        // Candidate min-heap (by distance) drives exploration. Every unvisited
        // neighbor is pushed so the beam keeps walking the graph even when
        // its `W` set is full of nodes tied with the current candidate — a
        // late node with a strictly smaller distance can only be discovered
        // by walking through equal-distance intermediaries.
        let mut candidates: BinaryHeap<Reverse<(Dist, usize)>> = entry_points
            .iter()
            .map(|(d, n)| Reverse((*d, *n)))
            .collect();
        // Result max-heap (by distance, bounded to `ef`) collects the beam's
        // top hits. Ties break by `RowId` so the final sort is stable.
        let mut results: BinaryHeap<(Dist, RowId, usize)> = entry_points
            .iter()
            .map(|(d, n)| (*d, self.row_ids[*n], *n))
            .collect();
        while let Some(Reverse((cd, c))) = candidates.pop() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let worst = results.peek().map(|(d, _, _)| *d).unwrap_or(Dist::MAX);
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
                    // Always queue the neighbor for further exploration —
                    // even when the result heap is full. The strict-key
                    // admission check below would otherwise stop the beam at
                    // row_ids larger than the worst in `W` and hide strictly
                    // closer nodes reachable through equal-distance hops.
                    candidates.push(Reverse((d, e)));
                    let worst_key = results
                        .peek()
                        .map(|(distance, row_id, _)| (*distance, *row_id))
                        .unwrap_or((Dist::MAX, RowId(u64::MAX)));
                    let key = (d, self.row_ids[e]);
                    if key < worst_key || results.len() < ef {
                        results.push((d, self.row_ids[e], e));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        Ok(results
            .into_vec()
            .into_iter()
            .map(|(distance, _, node)| (distance, node))
            .collect())
    }

    /// Degree limit for `layer` (M at upper layers, 2M at layer 0).
    /// Test-only structural invariant accessor.
    pub fn degree_limit_for_layer(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m * 2
        } else {
            self.m
        }
    }

    /// Number of nodes reachable from the entry point at `layer` via a BFS
    /// over outgoing edges at that layer. Test-only connectivity probe (see
    /// the spec §7.3 structural invariant). Returns the row ids of every
    /// reachable node in BFS order.
    pub fn reachable_from_entry(&self, layer: usize) -> Vec<RowId> {
        let Some(entry) = self.entry else {
            return Vec::new();
        };
        if layer > self.max_level as usize {
            return Vec::new();
        }
        let mut visited: HashSet<usize> = HashSet::new();
        let mut queue: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
        visited.insert(entry);
        queue.push_back(entry);
        let mut order = Vec::new();
        while let Some(node) = queue.pop_front() {
            order.push(self.row_ids[node]);
            for &n in &self.graph[node][layer] {
                if visited.insert(n) {
                    queue.push_back(n);
                }
            }
        }
        order
    }

    /// Maximum degree observed at `layer` across every node. Test-only.
    pub fn max_adjacency_at(&self, layer: usize) -> usize {
        self.graph
            .iter()
            .filter(|adj| layer < adj.len())
            .map(|adj| adj[layer].len())
            .max()
            .unwrap_or(0)
    }

    /// Maximum top layer present in any node's graph. Test-only.
    pub fn top_layer(&self) -> i32 {
        self.max_level
    }

    /// Number of nodes in the graph. Test-only convenience.
    pub fn node_count(&self) -> usize {
        self.vectors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_exact_match_at_distance_zero() {
        let mut h = Hnsw::new(2, 8, 32);
        h.insert(vec![0b1010_1010, 0b0000_1111], RowId(100));
        h.insert(vec![0b0101_0101, 0b1111_0000], RowId(2));
        h.insert(vec![0b1010_1010, 0b0000_1111], RowId(1));
        let top = h.search(&[0b1010_1010, 0b0000_1111], 1, 32);
        assert_eq!(top[0].1, 0); // identical ⇒ distance 0
        assert_eq!(top[0].0, RowId(1));
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
            // Bridge-owner invariant (spec §7.4): closest selected neighbor is
            // the designated bridge. If every old neighbor prunes the
            // reciprocal edge, force the bridge to retain the new node while
            // preserving the degree bound.
            let bridge = neighbors.first().copied();
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
                    let evicted: Vec<usize> =
                        scored.iter().skip(m_layer).map(|(_, x)| *x).collect();
                    *adj = scored.iter().take(m_layer).map(|(_, x)| *x).collect();
                    // Cascading connectivity preservation (spec §7.4): if an
                    // evicted old node would lose its only incoming edge at
                    // this layer, re-establish a back-edge into the new node.
                    for &e in &evicted {
                        if e == node {
                            continue;
                        }
                        if !self.has_incoming_edge(e, lc as usize) {
                            self.preserve_into_new_node_adj_dense(node, e, lc as usize, m_layer);
                        }
                    }
                }
            }
            // Bridge-owner connectivity invariant (spec §7.4).
            let still_linked = neighbors
                .iter()
                .any(|&n| self.graph[n][lc as usize].contains(&node));
            if !still_linked {
                if let Some(bridge) = bridge {
                    let force_evicted =
                        self.compute_force_eviction_cosine(bridge, node, lc as usize, m_layer);
                    self.force_bounded_edge_cosine(bridge, node, lc as usize, m_layer);
                    if let Some(e) = force_evicted {
                        if e != node && !self.has_incoming_edge(e, lc as usize) {
                            self.preserve_into_new_node_adj_dense(node, e, lc as usize, m_layer);
                        }
                    }
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

    /// Force the bridge owner to retain `new_node` at `layer` while preserving
    /// the `degree_limit` cap (cosine distance). No-op if the owner already
    /// retains `new_node`; if full, replace the farthest retained edge.
    fn force_bounded_edge_cosine(
        &mut self,
        owner: usize,
        new_node: usize,
        layer: usize,
        degree_limit: usize,
    ) {
        let adj = &mut self.graph[owner][layer];
        if adj.contains(&new_node) {
            return;
        }
        if adj.len() < degree_limit {
            adj.push(new_node);
            return;
        }
        let owner_vec = self.vectors[owner].clone();
        let mut worst_pos: usize = 0;
        let mut worst_d: f32 = -1.0;
        for (pos, &x) in adj.iter().enumerate() {
            let d = cosine_distance(&owner_vec, &self.vectors[x]);
            if d >= worst_d {
                worst_d = d;
                worst_pos = pos;
            }
        }
        let new_d = cosine_distance(&owner_vec, &self.vectors[new_node]);
        if new_d < worst_d {
            adj[worst_pos] = new_node;
        } else {
            // Preserve the connectivity invariant by replacing the farthest
            // edge — the new node is reachable from the bridge owner at this
            // layer regardless of relative distance.
            adj[worst_pos] = new_node;
        }
    }

    /// Compute (without mutating) which entry would be evicted by
    /// [`force_bounded_edge_cosine`](Self::force_bounded_edge_cosine).
    fn compute_force_eviction_cosine(
        &self,
        owner: usize,
        new_node: usize,
        layer: usize,
        degree_limit: usize,
    ) -> Option<usize> {
        let adj = &self.graph[owner][layer];
        if adj.contains(&new_node) {
            return None;
        }
        if adj.len() < degree_limit {
            return None;
        }
        let owner_vec = &self.vectors[owner];
        let mut worst_pos: usize = 0;
        let mut worst_d: f32 = -1.0;
        for (pos, &x) in adj.iter().enumerate() {
            let d = cosine_distance(owner_vec, &self.vectors[x]);
            if d >= worst_d {
                worst_d = d;
                worst_pos = pos;
            }
        }
        Some(adj[worst_pos])
    }

    /// Whether `node` has at least one incoming edge at `layer` (any other
    /// node has it in their `graph[...][layer]` adjacency list). Used by
    /// the cascading connectivity preservation step.
    fn has_incoming_edge(&self, node: usize, layer: usize) -> bool {
        if layer > MAX_HNSW_LEVEL as usize {
            return false;
        }
        for adj in &self.graph {
            if layer < adj.len() && adj[layer].contains(&node) {
                return true;
            }
        }
        false
    }

    /// Re-establish a back-edge from the new node to `evicted_node` by
    /// adding `evicted_node` to `new_node`'s adjacency at `layer`. The
    /// degree bound is enforced via cosine distance.
    fn preserve_into_new_node_adj_dense(
        &mut self,
        new_node: usize,
        evicted_node: usize,
        layer: usize,
        degree_limit: usize,
    ) {
        let adj = &mut self.graph[new_node][layer];
        if adj.contains(&evicted_node) {
            return;
        }
        if adj.len() < degree_limit {
            adj.push(evicted_node);
            return;
        }
        let nv = self.vectors[new_node].clone();
        let mut worst_pos: usize = 0;
        let mut worst_d: f32 = -1.0;
        for (pos, &x) in adj.iter().enumerate() {
            let d = cosine_distance(&nv, &self.vectors[x]);
            if d >= worst_d {
                worst_d = d;
                worst_pos = pos;
            }
        }
        // Always replace — the connectivity invariant requires the evicted
        // node to remain reachable from the new node regardless of distance
        // (spec §7.4).
        adj[worst_pos] = evicted_node;
    }

    /// k-nearest neighbors of `query` (cosine distance). `ef` controls beam
    /// width (larger ⇒ higher recall).
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(RowId, f32)> {
        // Floor the beam at 2·k (not just k): the engine's candidate-cap loop
        // starts the first ANN probe at exactly k, which starves recall on
        // small top-k queries — the beam finishes before the graph's
        // closer-than-current-worst neighbors get expanded. 2·k matches the
        // recall budget used in the dense recall test.
        let ef = ef.max(k.saturating_mul(2));
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
        let ef = ef.max(k);
        if ef >= self.vectors.len() {
            let mut results = Vec::with_capacity(self.vectors.len());
            for (node, vector) in self.vectors.iter().enumerate() {
                if let Some(context) = context {
                    context.consume(crate::query::work_units(
                        self.dim,
                        crate::query::FLOAT_WORK_QUANTUM,
                    ))?;
                }
                results.push((cosine_distance(query, vector), node));
            }
            results.sort_by(|(da, na), (db, nb)| {
                da.total_cmp(db)
                    .then_with(|| self.row_ids[*na].cmp(&self.row_ids[*nb]))
            });
            return Ok(results
                .into_iter()
                .take(k)
                .map(|(distance, node)| (self.row_ids[node], distance))
                .collect());
        }
        if let Some(context) = context {
            context.consume(crate::query::work_units(
                self.dim,
                crate::query::FLOAT_WORK_QUANTUM,
            ))?;
        }
        let mut ep: Vec<(f32, usize)> = vec![(cosine_distance(query, &self.vectors[entry]), entry)];
        for lc in (1..=self.max_level).rev() {
            ep = self.search_layer_with_context(query, ep, 1, lc, context)?;
        }
        let mut results = self.search_layer_with_context(query, ep, ef, 0, context)?;
        results.sort_by(|(da, na), (db, nb)| {
            da.total_cmp(db)
                .then_with(|| self.row_ids[*na].cmp(&self.row_ids[*nb]))
        });
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
        let mut results: BinaryHeap<(DistF32, RowId, usize)> = entry_points
            .iter()
            .map(|(distance, node)| (DistF32(*distance), self.row_ids[*node], *node))
            .collect();

        while let Some(Reverse((candidate_distance, candidate))) = candidates.pop() {
            checkpoint()?;
            let worst = results
                .peek()
                .map(|(distance, _, _)| distance.0)
                .unwrap_or(f32::INFINITY);
            if candidate_distance.0 > worst && results.len() >= ef {
                break;
            }
            for &neighbor in &self.graph[candidate][layer as usize] {
                checkpoint()?;
                if visited.insert(neighbor) {
                    let distance = cosine_distance(query, &self.vectors[neighbor]);
                    // Always queue the neighbor for further exploration —
                    // even when the result heap is full. The strict-key
                    // admission check below would otherwise stop the beam at
                    // row_ids larger than the worst in `W` and hide strictly
                    // closer nodes reachable through equal-distance hops.
                    candidates.push(Reverse((DistF32(distance), neighbor)));
                    let worst_key = results
                        .peek()
                        .map(|(distance, row_id, _)| (*distance, *row_id))
                        .unwrap_or((DistF32(f32::INFINITY), RowId(u64::MAX)));
                    if (DistF32(distance), self.row_ids[neighbor]) < worst_key || results.len() < ef
                    {
                        results.push((DistF32(distance), self.row_ids[neighbor], neighbor));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        Ok(results
            .into_iter()
            .map(|(distance, _, node)| (distance.0, node))
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
        let mut results: BinaryHeap<(DistF32, RowId, usize)> = entry_points
            .iter()
            .map(|(d, n)| (DistF32(*d), self.row_ids[*n], *n))
            .collect();
        while let Some(Reverse((cd, c))) = candidates.pop() {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let worst = results.peek().map(|(d, _, _)| d.0).unwrap_or(f32::INFINITY);
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
                    // Always queue the neighbor for further exploration —
                    // even when the result heap is full. The strict-key
                    // admission check below would otherwise stop the beam at
                    // row_ids larger than the worst in `W` and hide strictly
                    // closer nodes reachable through equal-distance hops.
                    candidates.push(Reverse((DistF32(d), e)));
                    let worst_key = results
                        .peek()
                        .map(|(distance, row_id, _)| (*distance, *row_id))
                        .unwrap_or((DistF32(f32::INFINITY), RowId(u64::MAX)));
                    if (DistF32(d), self.row_ids[e]) < worst_key || results.len() < ef {
                        results.push((DistF32(d), self.row_ids[e], e));
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        Ok(results.into_iter().map(|(d, _, n)| (d.0, n)).collect())
    }

    /// Degree limit for `layer` (M at upper layers, 2M at layer 0).
    /// Test-only structural invariant accessor.
    pub fn degree_limit_for_layer(&self, layer: usize) -> usize {
        if layer == 0 {
            self.m * 2
        } else {
            self.m
        }
    }

    /// Number of nodes reachable from the entry point at `layer` via a BFS
    /// over outgoing edges at that layer. Test-only connectivity probe (see
    /// the spec §7.3 structural invariant). Returns the row ids of every
    /// reachable node in BFS order.
    pub fn reachable_from_entry(&self, layer: usize) -> Vec<RowId> {
        let Some(entry) = self.entry else {
            return Vec::new();
        };
        if layer > self.max_level as usize {
            return Vec::new();
        }
        let mut visited: HashSet<usize> = HashSet::new();
        let mut queue: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
        visited.insert(entry);
        queue.push_back(entry);
        let mut order = Vec::new();
        while let Some(node) = queue.pop_front() {
            order.push(self.row_ids[node]);
            for &n in &self.graph[node][layer] {
                if visited.insert(n) {
                    queue.push_back(n);
                }
            }
        }
        order
    }

    /// Maximum degree observed at `layer` across every node. Test-only.
    pub fn max_adjacency_at(&self, layer: usize) -> usize {
        self.graph
            .iter()
            .filter(|adj| layer < adj.len())
            .map(|adj| adj[layer].len())
            .max()
            .unwrap_or(0)
    }

    /// Maximum top layer present in any node's graph. Test-only.
    pub fn top_layer(&self) -> i32 {
        self.max_level
    }

    /// Number of nodes in the graph. Test-only convenience.
    pub fn node_count(&self) -> usize {
        self.vectors.len()
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
        h.insert(vec![1.0, 0.0, 0.0], RowId(100));
        h.insert(vec![0.0, 1.0, 0.0], RowId(2));
        h.insert(vec![1.0, 0.0, 0.0], RowId(1));
        let top = h.search(&[1.0, 0.0, 0.0], 1, 32);
        assert_eq!(top[0].1, 0.0);
        assert_eq!(top[0].0, RowId(1));
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

    /// Dense ANN top-k after a churn of deletes + reinserts through the
    /// `search_filtered` visibility filter. HNSW has no cheap node removal,
    /// so a "delete" is modelled by excluding the row id from the visibility
    /// predicate; a "reinsert" hands the index a fresh rid. The two searches
    /// assert the merged HNSW stays consistent with brute-force cosine
    /// ranking across the visible set (regression target for the
    /// `churn_oracle_ann_hnsw_dense` integration oracle).
    #[test]
    fn dense_topk_after_delete_and_reinsert() {
        use crate::index::AnnIndex;
        use crate::schema::AnnQuantization;

        let dim = 8;
        let mut index = AnnIndex::with_quantization(dim, 16, 64, 64, AnnQuantization::Dense);

        // Deterministic signed-random generator so the assertions are stable.
        let mut seed = 0xC0FFEE_1234_5678u64;
        let mut next = |s: &mut u64| {
            *s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = ((*s >> 33) as u32) as f32 / (u32::MAX as f32);
            u * 2.0 - 1.0
        };

        // 1. Insert 100 vectors with known embeddings (rids 0..100).
        let mut data: Vec<(Vec<f32>, RowId)> = Vec::with_capacity(150);
        for i in 0..100u64 {
            let v: Vec<f32> = (0..dim).map(|_| next(&mut seed)).collect();
            let rid = RowId(i);
            data.push((v.clone(), rid));
            index.insert(&v, rid).unwrap();
        }

        let brute_topk =
            |query: &[f32], k: usize, pool: &[(Vec<f32>, RowId)]| -> Vec<(RowId, f32)> {
                let mut scored: Vec<(RowId, f32)> = pool
                    .iter()
                    .map(|(v, rid)| (*rid, cosine_distance(query, v)))
                    .collect();
                scored.sort_by(|(ra, da), (rb, db)| da.total_cmp(db).then_with(|| ra.0.cmp(&rb.0)));
                scored.truncate(k);
                scored
            };

        // 2. Search top-10 — the 10 closest vectors by cosine among all 100.
        let query = data[0].0.clone();
        let expected_first = brute_topk(&query, 10, &data);
        let first = index.search_filtered(&query, 10, &|_: RowId| true).unwrap();
        assert_eq!(
            first.len(),
            10,
            "first top-10 should return 10 hits (got {})",
            first.len()
        );
        for (rank, (got_rid, _)) in first.iter().enumerate() {
            assert_eq!(
                got_rid.0, expected_first[rank].0 .0,
                "first-search rank {rank}: got rid {} expected {} (brute: {:?})",
                got_rid.0, expected_first[rank].0 .0, expected_first
            );
        }

        // 3. Delete 50 — visibility excludes the first 50 rids.
        let deleted: HashSet<u64> = (0..50).collect();
        let visible = |rid: RowId| !deleted.contains(&rid.0);

        // 4. Re-insert 50 new vectors with fresh rids (100..150).
        for i in 0..50u64 {
            let v: Vec<f32> = (0..dim).map(|_| next(&mut seed)).collect();
            let rid = RowId(100 + i);
            data.push((v.clone(), rid));
            index.insert(&v, rid).unwrap();
        }

        // 5. Search top-10 again — brute-force over the visible set
        //    (50 original non-deleted + 50 newly-inserted = 100).
        let visible_pool: Vec<(Vec<f32>, RowId)> = data
            .iter()
            .filter(|(_, rid)| !deleted.contains(&rid.0))
            .cloned()
            .collect();
        let expected_second = brute_topk(&query, 10, &visible_pool);
        let second = index.search_filtered(&query, 10, &visible).unwrap();
        assert_eq!(
            second.len(),
            10,
            "second top-10 should return 10 hits (got {})",
            second.len()
        );
        for (rank, (got_rid, _)) in second.iter().enumerate() {
            assert_eq!(
                got_rid.0, expected_second[rank].0 .0,
                "second-search rank {rank}: got rid {} expected {} (brute: {:?})",
                got_rid.0, expected_second[rank].0 .0, expected_second
            );
        }
        // No result may be one of the deleted rids.
        for (rid, _) in &second {
            assert!(
                !deleted.contains(&rid.0),
                "deleted rid {} leaked into second top-10",
                rid.0
            );
        }
    }
}
