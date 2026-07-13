//! Manual/nightly AI retrieval characterization. No wall-clock assertions.

use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Retriever, RetrieverScore, SearchRequest, SetMember,
    SetSimilarityRequest,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use std::collections::HashSet;
use std::time::Instant;

const DIM: usize = 64;

fn embedding(id: usize) -> Vec<f32> {
    (0..DIM)
        .map(|dimension| {
            let mut value = (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ (dimension as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            value ^= value >> 31;
            ((value >> 40) as f32 / (1u32 << 24) as f32) * 2.0 - 1.0
        })
        .collect()
}

fn sparse(id: usize) -> Vec<(u32, f32)> {
    (0..4)
        .map(|offset| {
            (
                ((id + offset * 997) % 8192) as u32,
                1.0 / (offset + 1) as f32,
            )
        })
        .collect()
}

fn members(id: usize) -> Vec<String> {
    member_ids(id)
        .into_iter()
        .map(|member| format!("t{member}"))
        .collect()
}

fn member_ids(id: usize) -> [usize; 8] {
    std::array::from_fn(|offset| (id + offset * 31) % 16_384)
}

fn exact_jaccard(left: usize, right: usize) -> f32 {
    let left = member_ids(left);
    let right = member_ids(right);
    let intersection = left.iter().filter(|member| right.contains(member)).count();
    intersection as f32 / (16 - intersection) as f32
}

fn schema() -> Schema {
    let column = |id: u16, name: &str, ty: TypeId, primary_key: bool| ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary_key {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
    };
    Schema {
        schema_id: 1,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "status", TypeId::Bytes, false),
            column(3, "embedding", TypeId::Embedding { dim: DIM as u32 }, false),
            column(4, "sparse", TypeId::Bytes, false),
            column(5, "members", TypeId::Bytes, false),
        ],
        indexes: vec![
            index("status", 2, IndexKind::Bitmap),
            index("embedding", 3, IndexKind::Ann),
            index("sparse", 4, IndexKind::Sparse),
            index("members", 5, IndexKind::MinHash),
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn index(name: &str, column_id: u16, kind: IndexKind) -> IndexDef {
    IndexDef {
        name: name.into(),
        column_id,
        kind,
        predicate: None,
        options: Default::default(),
    }
}

fn percentile(values: &mut [u128], fraction: f64) -> u128 {
    values.sort_unstable();
    values[((values.len() - 1) as f64 * fraction).round() as usize]
}

fn recall(got: impl Iterator<Item = u64>, truth: &HashSet<u64>, k: usize) -> f64 {
    got.take(k).filter(|row_id| truth.contains(row_id)).count() as f64 / k as f64
}

fn main() {
    let rows = std::env::var("MONGRELDB_AI_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000usize);
    let queries = std::env::var("MONGRELDB_AI_BENCH_QUERIES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(50usize);
    let dir = tempfile::tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema(), 1).unwrap();
    let build_started = Instant::now();
    for id in 0..rows {
        table
            .put(vec![
                (1, Value::Int64(id as i64)),
                (
                    2,
                    Value::Bytes(
                        if id % 2 == 0 {
                            b"even".as_slice()
                        } else {
                            b"odd".as_slice()
                        }
                        .to_vec(),
                    ),
                ),
                (3, Value::Embedding(embedding(id))),
                (4, Value::Bytes(bincode::serialize(&sparse(id)).unwrap())),
                (5, Value::Bytes(serde_json::to_vec(&members(id)).unwrap())),
            ])
            .unwrap();
    }
    table.commit().unwrap();
    table.close().unwrap();
    let build_ms = build_started.elapsed().as_millis();
    for id in 0..5.min(rows) {
        table
            .retrieve(&Retriever::Ann {
                column_id: 3,
                query: embedding(id),
                k: 10,
            })
            .unwrap();
        table
            .retrieve(&Retriever::Sparse {
                column_id: 4,
                query: sparse(id),
                k: 10,
            })
            .unwrap();
        table
            .retrieve(&Retriever::MinHash {
                column_id: 5,
                members: members(id).into_iter().map(SetMember::String).collect(),
                k: 10,
            })
            .unwrap();
    }
    let mut ann_us = Vec::new();
    let mut sparse_us = Vec::new();
    let mut minhash_us = Vec::new();
    let mut minhash_verify_us = Vec::new();
    let mut minhash_verify_gather_us = Vec::new();
    let mut minhash_verify_parse_us = Vec::new();
    let mut minhash_verify_score_us = Vec::new();
    let mut hybrid_us = Vec::new();
    let mut graph_recall = 0.0;
    let mut cosine_recall = 0.0;
    let mut minhash_candidate_recall = 0.0;
    let mut minhash_candidate_count = 0usize;
    let mut minhash_error = 0.0f64;
    let mut minhash_error_samples = 0usize;
    let mut sparse_postings_visited = 0usize;
    let mut hybrid_union_size = 0usize;

    for query_number in 0..queries {
        let id = query_number * rows / queries;
        let vector = embedding(id);
        let ann = Retriever::Ann {
            column_id: 3,
            query: vector.clone(),
            k: 10,
        };
        let started = Instant::now();
        let ann_hits = table.retrieve(&ann).unwrap();
        ann_us.push(started.elapsed().as_micros());

        let mut hamming_truth: Vec<_> = (0..rows)
            .map(|row_id| {
                let candidate = embedding(row_id);
                let distance = vector
                    .iter()
                    .zip(candidate)
                    .filter(|(left, right)| (**left > 0.0) != (*right > 0.0))
                    .count();
                (distance, row_id as u64)
            })
            .collect();
        hamming_truth.sort_unstable();
        let hamming_truth: HashSet<_> = hamming_truth
            .into_iter()
            .take(10)
            .map(|(_, id)| id)
            .collect();
        graph_recall += recall(ann_hits.iter().map(|hit| hit.row_id.0), &hamming_truth, 10);

        let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
        let mut cosine_truth: Vec<_> = (0..rows)
            .map(|row_id| {
                let candidate = embedding(row_id);
                let candidate_norm = candidate
                    .iter()
                    .map(|value| value * value)
                    .sum::<f32>()
                    .sqrt();
                let score = vector
                    .iter()
                    .zip(candidate)
                    .map(|(left, right)| left * right)
                    .sum::<f32>()
                    / (norm * candidate_norm);
                (score, row_id as u64)
            })
            .collect();
        cosine_truth.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let cosine_truth: HashSet<_> = cosine_truth
            .into_iter()
            .take(10)
            .map(|(_, id)| id)
            .collect();
        cosine_recall += recall(ann_hits.iter().map(|hit| hit.row_id.0), &cosine_truth, 10);

        let sparse_retriever = Retriever::Sparse {
            column_id: 4,
            query: sparse(id),
            k: 10,
        };
        let started = Instant::now();
        let sparse_hits = table.retrieve(&sparse_retriever).unwrap();
        sparse_us.push(started.elapsed().as_micros());
        let query_tokens: HashSet<_> = sparse(id).into_iter().map(|(token, _)| token).collect();
        sparse_postings_visited += (0..rows)
            .map(|row_id| {
                sparse(row_id)
                    .iter()
                    .filter(|(token, _)| query_tokens.contains(token))
                    .count()
            })
            .sum::<usize>();
        let member_values: Vec<_> = members(id).into_iter().map(SetMember::String).collect();
        let minhash = Retriever::MinHash {
            column_id: 5,
            members: member_values.clone(),
            k: 10,
        };
        let started = Instant::now();
        let minhash_hits = table.retrieve(&minhash).unwrap();
        minhash_us.push(started.elapsed().as_micros());
        minhash_candidate_count += minhash_hits.len();
        let mut truth: Vec<_> = (0..rows)
            .map(|row_id| (exact_jaccard(id, row_id), row_id as u64))
            .collect();
        truth.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let truth: HashSet<_> = truth
            .into_iter()
            .take(10)
            .map(|(_, row_id)| row_id)
            .collect();
        minhash_candidate_recall += recall(minhash_hits.iter().map(|hit| hit.row_id.0), &truth, 10);
        for hit in &minhash_hits {
            let RetrieverScore::MinHashEstimatedJaccard(estimate) = hit.score else {
                unreachable!()
            };
            minhash_error += (estimate - exact_jaccard(id, hit.row_id.0 as usize)).abs() as f64;
            minhash_error_samples += 1;
        }
        let started = Instant::now();
        let (_, trace) = table
            .set_similarity_explained(&SetSimilarityRequest {
                column_id: 5,
                members: member_values,
                candidate_k: 100,
                min_jaccard: 0.0,
                limit: 100,
            })
            .unwrap();
        minhash_verify_us.push(started.elapsed().as_micros());
        minhash_verify_gather_us.push(trace.gather_us as u128);
        minhash_verify_parse_us.push(trace.parse_us as u128);
        minhash_verify_score_us.push(trace.score_us as u128);
        hybrid_union_size += ann_hits
            .iter()
            .map(|hit| hit.row_id)
            .chain(sparse_hits.iter().map(|hit| hit.row_id))
            .collect::<HashSet<_>>()
            .len();
        let started = Instant::now();
        table
            .search(&SearchRequest {
                must: vec![Condition::BitmapEq {
                    column_id: 2,
                    value: b"even".to_vec(),
                }],
                retrievers: vec![
                    NamedRetriever {
                        name: "dense".into(),
                        weight: 1.0,
                        retriever: ann,
                    },
                    NamedRetriever {
                        name: "sparse".into(),
                        weight: 1.0,
                        retriever: sparse_retriever,
                    },
                ],
                fusion: Fusion::ReciprocalRank { constant: 60 },
                limit: 10,
                projection: Some(vec![1]),
            })
            .unwrap();
        hybrid_us.push(started.elapsed().as_micros());
    }
    let checkpoint_bytes = std::fs::metadata(dir.path().join("_idx/global.idx"))
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    let index_payloads = mongreldb_core::global_idx::plaintext_record_sizes(dir.path())
        .unwrap()
        .into_iter()
        .map(|record| {
            serde_json::json!({
                "kind": record.kind,
                "column_id": record.column_id,
                "payload_bytes": record.payload_bytes,
                "payload_bytes_per_row": record.payload_bytes as f64 / rows as f64,
            })
        })
        .collect::<Vec<_>>();
    let base_table_bytes = std::fs::read_dir(dir.path().join("_runs"))
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum::<u64>();
    let git_sha = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default();
    let git_dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .is_some_and(|output| !output.stdout.is_empty());
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_default();
    println!(
        "{}",
        serde_json::json!({
            "git_sha": git_sha,
            "git_dirty": git_dirty,
            "hardware": {"arch": std::env::consts::ARCH, "os": std::env::consts::OS, "label": std::env::var("MONGRELDB_BENCH_HARDWARE").unwrap_or_default()},
            "rustc": rustc,
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "features": "default",
            "warmup_queries": 5.min(rows),
            "rows": rows,
            "queries": queries,
            "build_ms": build_ms,
            "checkpoint_bytes": checkpoint_bytes,
            "index_bytes_per_row": checkpoint_bytes as f64 / rows as f64,
            "base_table_bytes": base_table_bytes,
            "base_table_bytes_per_row": base_table_bytes as f64 / rows as f64,
            "index_payloads": index_payloads,
            "ann": {"p50_us": percentile(&mut ann_us, 0.50), "p95_us": percentile(&mut ann_us, 0.95), "hamming_recall_at_10": graph_recall / queries as f64, "cosine_recall_at_10": cosine_recall / queries as f64},
            "sparse": {"p50_us": percentile(&mut sparse_us, 0.50), "p95_us": percentile(&mut sparse_us, 0.95), "average_postings_visited": sparse_postings_visited as f64 / queries as f64},
            "minhash": {"p50_us": percentile(&mut minhash_us, 0.50), "p95_us": percentile(&mut minhash_us, 0.95), "verification_p50_us": percentile(&mut minhash_verify_us, 0.50), "verification_p95_us": percentile(&mut minhash_verify_us, 0.95), "verification_gather_p50_us": percentile(&mut minhash_verify_gather_us, 0.50), "verification_gather_p95_us": percentile(&mut minhash_verify_gather_us, 0.95), "verification_parse_p50_us": percentile(&mut minhash_verify_parse_us, 0.50), "verification_parse_p95_us": percentile(&mut minhash_verify_parse_us, 0.95), "verification_score_p50_us": percentile(&mut minhash_verify_score_us, 0.50), "verification_score_p95_us": percentile(&mut minhash_verify_score_us, 0.95), "candidate_recall_at_10": minhash_candidate_recall / queries as f64, "average_candidates": minhash_candidate_count as f64 / queries as f64, "estimated_exact_mean_absolute_error": minhash_error / minhash_error_samples.max(1) as f64},
            "hybrid": {"p50_us": percentile(&mut hybrid_us, 0.50), "p95_us": percentile(&mut hybrid_us, 0.95), "average_union_size": hybrid_union_size as f64 / queries as f64},
        })
    );
}
