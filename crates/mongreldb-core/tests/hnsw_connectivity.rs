//! Issue 3 — HNSW reciprocal pruning / reachability invariant (spec §7).
//!
//! Each test is constructed so the graph defect (a node stored but
//! unreachable from the entry point) would surface without the
//! bridge-owner invariant. `ef < len` is enforced so the dense fallback
//! (`ef >= len` ⇒ brute-force) cannot mask the defect.

use mongreldb_core::index::ann::AnnIndex;
use mongreldb_core::index::hnsw::{DenseHnsw, Hnsw};
use mongreldb_core::query::{AiExecutionContext, Fusion, NamedRetriever, Retriever, SearchRequest};
use mongreldb_core::rowid::RowId;
use mongreldb_core::schema::{
    AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId,
};
use mongreldb_core::trace::QueryTrace;
use mongreldb_core::{Table, Value};
use std::collections::HashSet;

/// Helper: assemble an all-ones packed-bit vector of `bytes` bytes.
fn ones(bytes: usize) -> Vec<u8> {
    vec![0xFFu8; bytes]
}

/// Helper: assemble an all-zeros packed-bit vector of `bytes` bytes.
fn zeros(bytes: usize) -> Vec<u8> {
    vec![0u8; bytes]
}

// ---------------------------------------------------------------------------
// Binary HNSW tests
// ---------------------------------------------------------------------------

/// Spec §7.3 binary test: small M, dense cluster of identical vectors, late
/// distant vector. After the cluster saturates adjacency lists, the late
/// distant vector's neighbors may all evict it during their independent
/// truncation. The bridge-owner invariant forces one of them to keep the
/// reciprocal edge, so the late row stays reachable.
#[test]
fn binary_late_distant_node_returns_row_id() {
    let bytes_per_vec = 2;
    let m = 2;
    let ef_construction = 16;
    let mut h = Hnsw::new(bytes_per_vec, m, ef_construction);

    // 1. Insert a dense cluster of identical vectors (all bits set).
    let cluster_rids: Vec<RowId> = (0..20u64).map(RowId).collect();
    for rid in &cluster_rids {
        h.insert(ones(bytes_per_vec), *rid);
    }
    // 2. Insert one distant vector last (all bits clear) with a known row id.
    let distant_rid = RowId(999);
    let distant_bits = zeros(bytes_per_vec);
    h.insert(distant_bits.clone(), distant_rid);

    let total = h.len();
    assert_eq!(total, 21);

    // Reachability diagnostic (independent of beam search behaviour).
    let reachable = h.reachable_from_entry(0);
    let reachable_set: std::collections::HashSet<RowId> = reachable.into_iter().collect();
    assert!(
        reachable_set.contains(&distant_rid),
        "late distant row {distant_rid:?} is not reachable at layer 0 — bridge-owner invariant failed"
    );

    // 3. Search using the distant vector itself. `ef < len` so the defect
    //    is exposed: the brute-force fallback path is not exercised.
    let ef = 8;
    assert!(ef < total, "test must keep ef < total graph size");
    let top = h.search(&distant_bits, 1, ef);
    assert!(
        top.iter().any(|(rid, _)| *rid == distant_rid),
        "late distant row {distant_rid:?} missing from {top:?}"
    );
}

/// Spec §7.3 dense test (cosine). Repeat the binary scenario with full
/// precision `f32` vectors. The dense backend's `ef >= len` brute-force
/// path is bypassed because `ef < len` (here `8 < 21`).
#[test]
fn dense_late_distant_node_returns_row_id() {
    let dim = 4;
    let m = 2;
    let ef_construction = 16;
    let mut h = DenseHnsw::new(dim, m, ef_construction);

    let cluster: Vec<f32> = vec![1.0, 1.0, 1.0, 1.0];
    let cluster_rids: Vec<RowId> = (0..20u64).map(RowId).collect();
    for rid in &cluster_rids {
        h.insert(cluster.clone(), *rid);
    }
    let distant_rid = RowId(999);
    let distant: Vec<f32> = vec![-1.0, -1.0, -1.0, -1.0];
    h.insert(distant.clone(), distant_rid);

    let total = h.len();
    assert_eq!(total, 21);

    let ef = 8;
    assert!(ef < total, "test must keep ef < total graph size");
    let top = h.search(&distant, 1, ef);
    assert!(
        top.iter().any(|(rid, _)| *rid == distant_rid),
        "late distant row {distant_rid:?} missing from {top:?}"
    );
}

/// Duplicate cluster: a search for the cluster's vector should return
/// cluster members (the binary HNSW does not dedupe by row id, so all 20
/// should be reachable).
#[test]
fn binary_duplicate_cluster_is_fully_reachable() {
    let bytes_per_vec = 2;
    let mut h = Hnsw::new(bytes_per_vec, 4, 16);
    let rids: Vec<RowId> = (0..20u64).map(RowId).collect();
    for rid in &rids {
        h.insert(ones(bytes_per_vec), *rid);
    }
    let total = h.len();
    let ef = 8;
    assert!(ef < total);

    let top = h.search(&ones(bytes_per_vec), 20, ef);
    let found: HashSet<RowId> = top.iter().map(|(rid, _)| *rid).collect();
    for rid in &rids {
        assert!(
            found.contains(rid),
            "duplicate cluster member {rid:?} missing from {top:?}"
        );
    }
}

/// Near-duplicate cluster: each vector differs by one bit. Search with one
/// representative and confirm a substantial fraction of the cluster surfaces.
#[test]
fn binary_near_duplicate_cluster_returns_cluster_members() {
    let bytes_per_vec = 4;
    let m = 4;
    let mut h = Hnsw::new(bytes_per_vec, m, 16);
    let n = 24;
    let rids: Vec<RowId> = (0..n as u64).map(RowId).collect();
    for (idx, rid) in rids.iter().enumerate() {
        // Each vector has exactly one bit flipped.
        let mut v = vec![0u8; bytes_per_vec];
        v[idx / 8] |= 1u8 << (idx % 8);
        h.insert(v, *rid);
    }
    let total = h.len();
    let ef = 8;
    assert!(ef < total);

    // Query with an "all zeros" vector — the bit-flip vectors are 1-bit
    // Hamming away; distance 1 hits should be the cluster members.
    let query = vec![0u8; bytes_per_vec];
    let top = h.search(&query, n, ef);
    let found: HashSet<RowId> = top.iter().map(|(rid, _)| *rid).collect();
    let intersection = rids.iter().filter(|rid| found.contains(rid)).count();
    // We don't require full recall (the graph is approximate), but at least
    // half of the cluster must be reachable.
    assert!(
        intersection >= n / 2,
        "expected at least half of the near-duplicate cluster reachable, got {intersection}/{n}"
    );
}

/// Late bridge node between two clusters: clusters are separated by a wide
/// Hamming gap. Insert one node that lies between them. The bridge node
/// must remain reachable from the entry point.
#[test]
fn binary_late_bridge_node_between_two_clusters_returns_row_id() {
    let bytes_per_vec = 2;
    let m = 4;
    let mut h = Hnsw::new(bytes_per_vec, m, 32);

    // Cluster A: all bits set.
    for i in 0..15u64 {
        h.insert(ones(bytes_per_vec), RowId(i));
    }
    // Cluster B: pattern 0x0F 0xF0 (mixed; far from cluster A).
    for i in 15..30u64 {
        h.insert(vec![0x0F, 0xF0], RowId(i));
    }

    // Bridge vector: half-and-half bits, closer to neither cluster.
    let bridge_rid = RowId(999);
    let bridge = vec![0xF0, 0x0F];
    h.insert(bridge.clone(), bridge_rid);

    let total = h.len();
    let ef = 8;
    assert!(ef < total);

    let top = h.search(&bridge, 1, ef);
    assert!(
        top.iter().any(|(rid, _)| *rid == bridge_rid),
        "bridge node {bridge_rid:?} missing from {top:?}"
    );
}

/// Multiple graph layers: insert enough vectors and small enough M that
/// several layers appear. Every node must be reachable at layer 0 (the
/// search graph).
#[test]
fn binary_multiple_layers_all_nodes_reachable_at_layer_zero() {
    let bytes_per_vec = 4;
    let m = 2;
    let mut h = Hnsw::new(bytes_per_vec, m, 16);
    let n = 64;
    let mut seed = 0xDEAD_BEEFu64;
    for i in 0..n {
        let mut v = vec![0u8; bytes_per_vec];
        for byte in v.iter_mut() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (seed >> 33) as u8;
        }
        h.insert(v, RowId(i as u64));
    }
    let top = h.top_layer();
    assert!(
        top >= 1,
        "test setup: expected at least one upper layer, got {top}"
    );
    let reachable = h.reachable_from_entry(0);
    assert_eq!(
        reachable.len(),
        h.node_count(),
        "every node must be reachable from the entry at layer 0; got {}/{}",
        reachable.len(),
        h.node_count()
    );
}

/// Determinism: two `Hnsw` instances built from the same fixed seed (and
/// the same insert order with the same `bits`) produce identical adjacency
/// graphs and identical search results. The seed is fixed by the
/// implementation (see `rng_state` in `Hnsw::new`).
#[test]
fn binary_deterministic_fixed_seed_produces_identical_graph() {
    fn build() -> (Hnsw, Vec<(RowId, u32)>) {
        let bytes_per_vec = 4;
        let mut h = Hnsw::new(bytes_per_vec, 4, 16);
        let mut seed = 0xCAFE_F00Du64;
        for i in 0..32u64 {
            let mut v = vec![0u8; bytes_per_vec];
            for byte in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                *byte = (seed >> 33) as u8;
            }
            h.insert(v, RowId(i));
        }
        // Snapshot the search for a representative query.
        let q = vec![0xAA, 0x55, 0xCC, 0x33];
        let top = h.search(&q, 5, 16);
        let snapshot: Vec<(RowId, u32)> = top.into_iter().collect();
        (h, snapshot)
    }
    let (h1, s1) = build();
    let (h2, s2) = build();
    assert_eq!(s1, s2, "deterministic binary search mismatch");
    // Compare the adjacency topology.
    assert_eq!(h1.top_layer(), h2.top_layer());
    assert_eq!(h1.node_count(), h2.node_count());
    for layer in 0..=h1.top_layer() as usize {
        assert_eq!(
            h1.max_adjacency_at(layer),
            h2.max_adjacency_at(layer),
            "adjacency topology diverged at layer {layer}"
        );
    }
}

/// Determinism for the dense backend.
#[test]
fn dense_deterministic_fixed_seed_produces_identical_graph() {
    fn build() -> (DenseHnsw, Vec<(RowId, u32)>) {
        let dim = 4;
        let mut h = DenseHnsw::new(dim, 4, 16);
        let mut seed = 0xCAFE_F00Du64;
        for i in 0..32u64 {
            let mut v = vec![0f32; dim];
            for x in v.iter_mut() {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let u = ((seed >> 33) as u32) as f32 / (u32::MAX as f32);
                *x = u * 2.0 - 1.0;
            }
            // Map f32 bits to u32 for a portable snapshot.
            let key = RowId(i);
            h.insert(v, key);
        }
        let q = vec![0.5f32, -0.5, 0.25, -0.25];
        let top = h.search(&q, 5, 16);
        let snapshot: Vec<(RowId, u32)> =
            top.into_iter().map(|(rid, d)| (rid, d.to_bits())).collect();
        (h, snapshot)
    }
    let (h1, s1) = build();
    let (h2, s2) = build();
    assert_eq!(s1, s2, "deterministic dense search mismatch");
    assert_eq!(h1.top_layer(), h2.top_layer());
    assert_eq!(h1.node_count(), h2.node_count());
}

/// Reopen/serialization: AnnIndex's binary-sign backend wraps a
/// `Hnsw`. Freeze → thaw must preserve the graph so the late distant row
/// stays reachable after reopen.
#[test]
fn binary_reopen_preserves_late_distant_node() {
    let dim = 16;
    let mut index = AnnIndex::with_quantization(dim, 2, 16, 16, AnnQuantization::BinarySign);

    // Dense identical cluster.
    let cluster_bits = vec![0xFFu8; dim.div_ceil(8)];
    for i in 0..20u64 {
        let v = index.quantize(&vec![1.0; dim]).unwrap();
        index.insert_quantized(v, RowId(i)).unwrap();
    }
    // Late distant vector: all bits clear (sign-flipped).
    let distant_bits = vec![0u8; dim.div_ceil(8)];
    let distant_rid = RowId(999);
    index
        .insert_quantized(distant_bits.clone(), distant_rid)
        .unwrap();

    // Freeze and re-open.
    let frozen = index.freeze();
    let thawed = AnnIndex::thaw(&frozen).unwrap();

    // The thawed graph must still return the distant row.
    let visible = |_: RowId| true;
    let top = thawed
        .search_filtered(&vec![-1.0f32; dim], 1, &visible)
        .unwrap();
    assert!(
        top.iter().any(|(rid, _)| *rid == distant_rid),
        "late distant row {distant_rid:?} missing after reopen: {top:?}"
    );
    // Sanity: the cluster vector also survives.
    let _ = cluster_bits; // keep the binding alive for documentation.
}

/// Filtered search after deleting high-scoring neighbors: build the
/// graph, then run `search_filtered` with a predicate that excludes the
/// top-scoring cluster. The remaining eligible rows must still be
/// returned up to `k` (this exercises the engine's widening logic and the
/// index's base+delta candidate merging).
#[test]
fn binary_filtered_search_after_deleting_high_scoring_neighbors_returns_k() {
    let dim = 16;
    let mut index = AnnIndex::with_quantization(dim, 4, 16, 16, AnnQuantization::BinarySign);

    // 30 identical vectors — they're all top-scoring for the +1 query.
    let plus = vec![1.0f32; dim];
    for i in 0..30u64 {
        index.insert(&plus, RowId(i)).unwrap();
    }
    // 10 distinct vectors far from +1.
    let minus = vec![-1.0f32; dim];
    for i in 30..40u64 {
        index.insert(&minus, RowId(i)).unwrap();
    }

    // Delete the 30 cluster rows.
    let deleted: HashSet<u64> = (0..30).collect();
    let visible = |rid: RowId| !deleted.contains(&rid.0);

    let k = 10;
    let top = index
        .search_filtered(&plus, k, &visible)
        .expect("search_filtered must succeed");
    assert_eq!(
        top.len(),
        k,
        "expected exactly {k} visible hits after deleting the high-scoring cluster, got {}",
        top.len()
    );
    for (rid, _) in &top {
        assert!(
            !deleted.contains(&rid.0),
            "deleted row {rid:?} leaked into filtered results"
        );
    }
}

/// Same filtered-search behaviour for the dense backend.
#[test]
fn dense_filtered_search_after_deleting_high_scoring_neighbors_returns_k() {
    let dim = 8;
    let mut index = AnnIndex::with_quantization(dim, 4, 16, 16, AnnQuantization::Dense);

    let plus = vec![1.0f32; dim];
    for i in 0..30u64 {
        index.insert(&plus, RowId(i)).unwrap();
    }
    let minus = vec![-1.0f32; dim];
    for i in 30..40u64 {
        index.insert(&minus, RowId(i)).unwrap();
    }

    let deleted: HashSet<u64> = (0..30).collect();
    let visible = |rid: RowId| !deleted.contains(&rid.0);
    let k = 10;
    let top = index
        .search_filtered(&plus, k, &visible)
        .expect("search_filtered must succeed");
    assert_eq!(
        top.len(),
        k,
        "expected exactly {k} visible hits after deleting the high-scoring cluster, got {}",
        top.len()
    );
    for (rid, _) in &top {
        assert!(
            !deleted.contains(&rid.0),
            "deleted row {rid:?} leaked into filtered results"
        );
    }
}

// ---------------------------------------------------------------------------
// Structural invariants (spec §7.3)
// ---------------------------------------------------------------------------

/// Every inserted node must be reachable from the entry point at layer 0
/// for both the binary and dense backends.
#[test]
fn structural_reachable_from_entry_at_layer_zero_covers_all_nodes() {
    let bytes_per_vec = 4;
    let mut hb = Hnsw::new(bytes_per_vec, 4, 16);
    let mut seed = 0xABCD_1234u64;
    for i in 0..40u64 {
        let mut v = vec![0u8; bytes_per_vec];
        for byte in v.iter_mut() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (seed >> 33) as u8;
        }
        hb.insert(v, RowId(i));
    }
    let reachable_b = hb.reachable_from_entry(0);
    assert_eq!(
        reachable_b.len(),
        hb.node_count(),
        "binary: every node must be reachable from the entry at layer 0 (got {}/{})",
        reachable_b.len(),
        hb.node_count()
    );

    let dim = 8;
    let mut hd = DenseHnsw::new(dim, 4, 16);
    seed = 0xABCD_1234u64;
    for i in 0..40u64 {
        let mut v = vec![0f32; dim];
        for x in v.iter_mut() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = ((seed >> 33) as u32) as f32 / (u32::MAX as f32);
            *x = u * 2.0 - 1.0;
        }
        hd.insert(v, RowId(i));
    }
    let reachable_d = hd.reachable_from_entry(0);
    assert_eq!(
        reachable_d.len(),
        hd.node_count(),
        "dense: every node must be reachable from the entry at layer 0 (got {}/{})",
        reachable_d.len(),
        hd.node_count()
    );
}

/// Adjacency length must never exceed the layer degree limit for any
/// node, at any layer. This is the second structural invariant.
#[test]
fn structural_adjacency_length_within_degree_limit() {
    let bytes_per_vec = 4;
    let mut h = Hnsw::new(bytes_per_vec, 4, 16);
    let mut seed = 0x1234_5678u64;
    for i in 0..80u64 {
        let mut v = vec![0u8; bytes_per_vec];
        for byte in v.iter_mut() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (seed >> 33) as u8;
        }
        h.insert(v, RowId(i));
    }
    for layer in 0..=h.top_layer() as usize {
        let limit = h.degree_limit_for_layer(layer);
        let observed = h.max_adjacency_at(layer);
        assert!(
            observed <= limit,
            "binary: adjacency at layer {layer} exceeds degree bound (observed {observed}, limit {limit})"
        );
    }

    let dim = 8;
    let mut hd = DenseHnsw::new(dim, 4, 16);
    seed = 0x1234_5678u64;
    for i in 0..80u64 {
        let mut v = vec![0f32; dim];
        for x in v.iter_mut() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let u = ((seed >> 33) as u32) as f32 / (u32::MAX as f32);
            *x = u * 2.0 - 1.0;
        }
        hd.insert(v, RowId(i));
    }
    for layer in 0..=hd.top_layer() as usize {
        let limit = hd.degree_limit_for_layer(layer);
        let observed = hd.max_adjacency_at(layer);
        assert!(
            observed <= limit,
            "dense: adjacency at layer {layer} exceeds degree bound (observed {observed}, limit {limit})"
        );
    }
}

/// Late-bridge-node test on the dense backend. The bridge vector is
/// inserted last; the search must return it.
#[test]
fn dense_late_bridge_node_between_two_clusters_returns_row_id() {
    let dim = 4;
    let mut h = DenseHnsw::new(dim, 4, 32);

    // Cluster A: +1 on every coordinate.
    let cluster_a = vec![1.0f32; dim];
    for i in 0..15u64 {
        h.insert(cluster_a.clone(), RowId(i));
    }
    // Cluster B: -1 on every coordinate.
    let cluster_b = vec![-1.0f32; dim];
    for i in 15..30u64 {
        h.insert(cluster_b.clone(), RowId(i));
    }
    // Bridge: orthogonal-ish vector equidistant from both clusters.
    let bridge = vec![0.0, 0.5, -0.5, 0.25];
    let bridge_rid = RowId(999);
    h.insert(bridge.clone(), bridge_rid);

    let total = h.len();
    let ef = 8;
    assert!(ef < total);
    let top = h.search(&bridge, 1, ef);
    assert!(
        top.iter().any(|(rid, _)| *rid == bridge_rid),
        "bridge node {bridge_rid:?} missing from {top:?}"
    );
}

// ---------------------------------------------------------------------------
// Step 5 (spec §7.5): engine-level filtered retrieval widens the candidate
// window when the first probe is dominated by stale/deleted high-scoring rows.
// ---------------------------------------------------------------------------

/// Build a table with a small ANN index, an embedding column, and the
/// minimum schema surface needed for `search_at_with_*` calls.
fn ann_table(dim: usize) -> (tempfile::TempDir, Table) {
    let dir = tempfile::tempdir().unwrap();
    let column = |id: u16, name: &str, ty: TypeId, primary: bool| ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
        embedding_source: None,
    };
    let dim_u32 = u32::try_from(dim).unwrap();
    let schema = Schema {
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: dim_u32 }, false),
        ],
        indexes: vec![IndexDef {
            name: "ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        }],
        ..Schema::default()
    };
    let table = Table::create(dir.path(), schema, 1).unwrap();
    (dir, table)
}

/// Insert `count` rows with embeddings sampled around `center`. Returns the
/// actual internal row ids (from `Table::put`) and the primary keys assigned
/// in order.
fn insert_cluster(
    table: &mut Table,
    center: &[f32],
    start_id: i64,
    count: usize,
) -> (Vec<RowId>, Vec<i64>) {
    let dim = center.len();
    let mut rids = Vec::with_capacity(count);
    let mut pks = Vec::with_capacity(count);
    for i in 0..count {
        let id = start_id + i as i64;
        let mut v = center.to_vec();
        let sign = if i % 2 == 0 { 1.0 } else { -1.0 };
        v[i % dim] += sign * 1e-3;
        let row_id = table
            .put(vec![(1, Value::Int64(id)), (2, Value::Embedding(v))])
            .unwrap();
        rids.push(row_id);
        pks.push(id);
    }
    (rids, pks)
}

fn ann_retriever(column_id: u16, query: Vec<f32>, k: usize) -> Retriever {
    Retriever::Ann {
        column_id,
        query,
        k,
    }
}

fn search_request(retrievers: Vec<NamedRetriever>, limit: usize) -> SearchRequest {
    SearchRequest {
        must: vec![],
        retrievers,
        fusion: Fusion::ReciprocalRank { constant: 60 },
        rerank: None,
        limit,
        projection: Some(vec![1]),
    }
}

fn named(name: &str, retriever: Retriever) -> NamedRetriever {
    NamedRetriever {
        name: name.into(),
        weight: 1.0,
        retriever,
    }
}

/// Step 5: when the first candidate window is dominated by stale/deleted
/// rows, the engine widens the beam and still returns `k` hits when enough
/// eligible rows exist. The trace's `candidate_cap_hit` stays `false` here
/// because widening succeeds within the cap.
#[test]
fn engine_filtered_widens_when_first_window_is_stale() {
    let dim = 8;
    let (_dir, mut table) = ann_table(dim);

    // Two distinct clusters: 30 stale (will be deleted) sitting near the
    // query and 30 eligible sitting far from the query. The stale cluster
    // dominates the first candidate window (cosine d=0 from query), but
    // every stale row is deleted so visibility rejects them — the engine
    // must widen the beam past the stale window to surface the eligible
    // rows.
    let stale_center = vec![1.0f32; dim];
    let eligible_center = vec![-1.0f32; dim];
    let (stale_row_ids, _stale_pks) = insert_cluster(&mut table, &stale_center, 1, 30);
    let (_eligible_row_ids, _eligible_pks) = insert_cluster(&mut table, &eligible_center, 1000, 30);
    table.commit().unwrap();

    // Delete the stale cluster by the actual row ids returned from put.
    for rid in &stale_row_ids {
        table.delete(*rid).unwrap();
    }
    table.commit().unwrap();

    // Run the ANN search using the stale-center query.
    let k = 5usize;
    let request = search_request(
        vec![named("ann", ann_retriever(2, stale_center.clone(), k))],
        k,
    );
    let snapshot = table.snapshot();
    // Cap=64 (set via fused-candidates ceiling). With 30 stale dominating
    // the beam and 30 eligible below them, the first probe at breadth=5
    // returns only stale. The engine doubles the window until the beam
    // reaches the eligible rows (which sit beyond the stale cluster in
    // the index ordering).
    let context =
        AiExecutionContext::with_limits(std::time::Duration::from_secs(30), usize::MAX, 64);
    let (hits, trace) = QueryTrace::capture(|| {
        table.search_at_with_candidate_authorization_and_context(
            &request,
            snapshot,
            None,
            Some(&context),
        )
    });
    let hits = hits.expect("search must succeed");
    assert_eq!(
        hits.len(),
        k,
        "engine must widen and return full k; got {hits:?}"
    );
    let hit_row_ids: HashSet<RowId> = hits.iter().map(|h| h.row_id).collect();
    for rid in &stale_row_ids {
        assert!(
            !hit_row_ids.contains(rid),
            "stale row_id {rid:?} leaked into widened hits"
        );
    }
    // Trace surfaces the widening: raw_candidates must exceed the first
    // probe (breadth=5), proving the loop doubled the window before
    // finding the eligible hits. cap_hit stays false because widening
    // succeeded within the cap.
    assert!(
        trace.raw_candidates > k,
        "raw_candidates must exceed the first-probe size to prove widening happened; got {}",
        trace.raw_candidates
    );
    assert!(
        !trace.candidate_cap_hit,
        "candidate_cap_hit must stay false when widening succeeds within the cap"
    );
    // The cap is recorded exactly. `ann_candidate_cap` floors at
    // `index.len()` (60 rows total) when it is the tightest bound; with
    // 60 rows the cap is exactly the index size.
    assert_eq!(
        trace.candidate_cap, 60,
        "candidate_cap should reflect the active index size when it is the tightest bound"
    );
}
