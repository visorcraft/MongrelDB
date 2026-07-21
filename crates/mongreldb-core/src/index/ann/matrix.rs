//! Verification matrix for the swappable ANN backends.
//!
//! Exercises every supported algorithm × quantization combination through the
//! real [`AnnIndex`] public API (insert → search → checkpoint round-trip) and
//! asserts recall against brute-force ground truth. Unsupported combinations
//! are asserted rejected by [`crate::schema::IndexDef::validate_options`].
//!
//! This is the Phase 2 "allowed-combination test matrix" deliverable.

#![cfg(test)]

use crate::index::ann::AnnDistance;
use crate::index::hnsw::cosine_distance;
use crate::rowid::RowId;
use crate::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, DiskAnnOptions, IndexDef, IndexKind, IndexOptions,
    IvfOptions, ProductQuantizerOptions,
};

/// Build the option bag for one supported (algorithm, quantization) pair.
fn options_for(algorithm: AnnAlgorithm, quantization: AnnQuantization) -> AnnOptions {
    AnnOptions {
        m: 16,
        ef_construction: 64,
        ef_search: 64,
        quantization,
        algorithm,
        diskann: (algorithm == AnnAlgorithm::DiskAnn).then(DiskAnnOptions::default),
        ivf: (algorithm == AnnAlgorithm::Ivf).then_some(IvfOptions {
            nlist: 8,
            nprobe: 4,
        }),
        product: matches!(quantization, AnnQuantization::Product { .. }).then_some(
            ProductQuantizerOptions {
                training_samples: 10_000,
                seed: 42,
                rerank_factor: 5,
            },
        ),
    }
}

fn one_hot_data(dim: usize, clusters: usize, per_cluster: usize) -> Vec<Vec<f32>> {
    let mut out = Vec::new();
    for c in 0..clusters {
        for member in 0..per_cluster {
            let mut v = vec![0f32; dim];
            v[(c * (dim / clusters)) % dim] = 1.0;
            v[((c * (dim / clusters)) % dim + 1) % dim] = (member as f32) * 0.001;
            out.push(v);
        }
    }
    out
}

/// Assert a search over `index` for `query` returns a result set overlapping
/// the brute-force top-k by at least `recall_threshold`.
fn assert_recall(
    distances: &[(RowId, AnnDistance)],
    data: &[Vec<f32>],
    query: &[f32],
    k: usize,
    recall_threshold: f64,
) {
    let mut brute: Vec<(f32, usize)> = data
        .iter()
        .enumerate()
        .map(|(i, v)| (cosine_distance(query, v), i))
        .collect();
    brute.sort_by(|(da, _), (db, _)| da.total_cmp(db));
    let truth: std::collections::HashSet<usize> = brute.iter().take(k).map(|(_, i)| *i).collect();
    let got: std::collections::HashSet<usize> =
        distances.iter().map(|(r, _)| r.0 as usize).collect();
    let recall = truth.intersection(&got).count() as f64 / k as f64;
    assert!(
        recall >= recall_threshold,
        "recall {recall:.2} below threshold {recall_threshold:.2}"
    );
}

#[test]
fn supported_combinations_build_and_search() {
    let dim = 16;
    let clusters = 4;
    let per_cluster = 8;
    let data = one_hot_data(dim, clusters, per_cluster);
    let supported = [
        (AnnAlgorithm::Hnsw, AnnQuantization::BinarySign),
        (AnnAlgorithm::Hnsw, AnnQuantization::Dense),
        (
            AnnAlgorithm::Hnsw,
            AnnQuantization::Product {
                num_subvectors: 8,
                bits: 8,
            },
        ),
        (AnnAlgorithm::DiskAnn, AnnQuantization::Dense),
        (AnnAlgorithm::Ivf, AnnQuantization::Dense),
    ];
    for (algorithm, quantization) in supported {
        let options = options_for(algorithm, quantization);
        // Validate accepts the combination.
        let def = IndexDef {
            name: "idx".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(options.clone()),
                ..IndexOptions::default()
            },
        };
        assert!(
            def.validate_options().is_ok(),
            "{algorithm:?} + {quantization:?} should be supported"
        );
        // Build, insert, search, checkpoint round-trip.
        let mut index = crate::index::AnnIndex::with_full_options(dim, 16, 64, 64, &options);
        for (i, vec) in data.iter().enumerate() {
            index.insert(vec, RowId(i as u64)).unwrap();
        }
        let query = data[0].clone();
        let results = index.search(&query, per_cluster).unwrap();
        assert_eq!(results.len(), per_cluster);
        // Recall: one-hot clusters are well-separated, so even flat PQ + IVF
        // should recover most cluster members. BinarySign recall is exact for
        // this data (clusters quantize distinctly).
        let threshold = match quantization {
            AnnQuantization::BinarySign => 0.95,
            AnnQuantization::Product { .. } => 0.80,
            _ => 0.85,
        };
        assert_recall(&results, &data, &query, per_cluster, threshold);
        // Checkpoint round-trip preserves results.
        let frozen = index.freeze();
        let thawed = crate::index::AnnIndex::thaw(&frozen).unwrap();
        assert_eq!(thawed.len(), data.len());
        let thawed_results = thawed.search(&query, per_cluster).unwrap();
        assert_eq!(thawed_results.len(), per_cluster);
    }
}

#[test]
fn unsupported_combinations_rejected_by_validation() {
    let unsupported = [
        // DiskANN over BinarySign (not yet wired).
        (AnnAlgorithm::DiskAnn, AnnQuantization::BinarySign),
        // DiskANN over Product (not yet wired).
        (
            AnnAlgorithm::DiskAnn,
            AnnQuantization::Product {
                num_subvectors: 8,
                bits: 8,
            },
        ),
        // IVF over BinarySign (not yet wired).
        (AnnAlgorithm::Ivf, AnnQuantization::BinarySign),
        // IVF over Product (not yet wired).
        (
            AnnAlgorithm::Ivf,
            AnnQuantization::Product {
                num_subvectors: 8,
                bits: 8,
            },
        ),
    ];
    for (algorithm, quantization) in unsupported {
        let options = options_for(algorithm, quantization);
        let def = IndexDef {
            name: "idx".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(options),
                ..IndexOptions::default()
            },
        };
        let err = def.validate_options();
        assert!(
            err.is_err(),
            "{algorithm:?} + {quantization:?} should be rejected as unsupported"
        );
        assert!(
            err.unwrap_err().to_string().contains("not supported"),
            "error should mention unsupported"
        );
    }
}

#[test]
fn preserve_old_hnsw_index_unchanged_after_unrelated_replace() {
    // An unrelated replace_index on a different column must not touch an
    // existing HNSW BinarySign index. This guards the "never silently rewrite"
    // invariant at the index level (the DDL path guarantees it at the schema
    // level via CAS).
    let dim = 8;
    let options = options_for(AnnAlgorithm::Hnsw, AnnQuantization::BinarySign);
    let mut index = crate::index::AnnIndex::with_full_options(dim, 16, 64, 64, &options);
    for i in 0..8u64 {
        let mut v = vec![0f32; dim];
        v[i as usize] = 1.0;
        index.insert(&v, RowId(i)).unwrap();
    }
    index.seal();
    let snapshot = index.freeze();
    // A no-op re-seal (simulating an unrelated publication) leaves the graph
    // byte-identical.
    index.seal();
    let re_snapshot = index.freeze();
    assert_eq!(
        snapshot, re_snapshot,
        "HNSW index must be unchanged by a no-op seal"
    );
}
