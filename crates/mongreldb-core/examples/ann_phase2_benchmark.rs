use std::time::Instant;

use mongreldb_core::index::AnnIndex;
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, DiskAnnOptions, IvfOptions, ProductQuantizerOptions,
};
use mongreldb_core::RowId;

const DIM: usize = 64;
const ROWS: usize = 512;
const QUERIES: usize = 100;
const K: usize = 10;

fn options(name: &str) -> AnnOptions {
    let mut options = AnnOptions {
        m: 16,
        ef_construction: 64,
        ef_search: 64,
        ..Default::default()
    };
    match name {
        "hnsw-binary" => {}
        "hnsw-dense" => options.quantization = AnnQuantization::Dense,
        "flat-product" => {
            options.quantization = AnnQuantization::Product {
                num_subvectors: 16,
                bits: 8,
            };
            options.product = Some(ProductQuantizerOptions {
                training_samples: ROWS,
                seed: 42,
                rerank_factor: 5,
            });
        }
        "diskann-dense" => {
            options.algorithm = AnnAlgorithm::DiskAnn;
            options.quantization = AnnQuantization::Dense;
            options.diskann = Some(DiskAnnOptions::default());
        }
        "ivf-dense" => {
            options.algorithm = AnnAlgorithm::Ivf;
            options.quantization = AnnQuantization::Dense;
            options.ivf = Some(IvfOptions {
                nlist: 16,
                nprobe: 4,
                training_samples: ROWS,
            });
        }
        _ => panic!("unknown backend {name}"),
    }
    options
}

fn vector(row: usize) -> Vec<f32> {
    let mut state = row as u64 + 1;
    (0..DIM)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            ((state >> 32) as u32 as f32) / u32::MAX as f32 * 2.0 - 1.0
        })
        .collect()
}

fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        })
        .unwrap_or(0)
        * 1024
}

fn percentile(samples: &mut [u128], numerator: usize, denominator: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * numerator / denominator]
}

fn main() {
    let name = std::env::args().nth(1).expect("backend argument required");
    let options = options(&name);
    let build_started = Instant::now();
    let mut index = AnnIndex::try_with_full_options(
        DIM,
        options.m,
        options.ef_construction,
        options.ef_search,
        &options,
    )
    .unwrap();
    for row in 0..ROWS {
        index.insert(&vector(row), RowId(row as u64)).unwrap();
    }
    let checkpoint = index.freeze();
    let build_micros = build_started.elapsed().as_micros();
    let index = AnnIndex::thaw(&checkpoint).unwrap();
    let mut query_micros = Vec::with_capacity(QUERIES);
    for query in 0..QUERIES {
        let started = Instant::now();
        assert_eq!(index.search(&vector(query * 5), K).unwrap().len(), K);
        query_micros.push(started.elapsed().as_micros());
    }
    let mut p50 = query_micros.clone();
    let mut p95 = query_micros;
    println!(
        "{{\"backend\":\"{name}\",\"rows\":{ROWS},\"dim\":{DIM},\"build_micros\":{build_micros},\"peak_rss_bytes\":{},\"checkpoint_bytes\":{},\"query_p50_micros\":{},\"query_p95_micros\":{}}}",
        peak_rss_bytes(),
        checkpoint.len(),
        percentile(&mut p50, 50, 100),
        percentile(&mut p95, 95, 100),
    );
}
