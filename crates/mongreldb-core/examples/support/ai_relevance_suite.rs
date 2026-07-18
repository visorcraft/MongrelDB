use mongreldb_core::query::{
    AnnRerankRequest, Fusion, NamedRetriever, Rerank, Retriever, SearchRequest, VectorMetric,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Table, Value};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

const DIM: usize = 256;
const TOP_K: usize = 10;

const DOCUMENTS: &[(&str, &str)] = &[
    (
        "getting-started",
        include_str!("../../../../docs/01-getting-started.md"),
    ),
    (
        "rust-quickstart",
        include_str!("../../../../docs/02-rust-quickstart.md"),
    ),
    (
        "nodejs-quickstart",
        include_str!("../../../../docs/03-nodejs-quickstart.md"),
    ),
    (
        "sql-queries",
        include_str!("../../../../docs/04-sql-queries.md"),
    ),
    (
        "native-queries",
        include_str!("../../../../docs/05-native-queries.md"),
    ),
    ("indexes", include_str!("../../../../docs/06-indexes.md")),
    (
        "encryption",
        include_str!("../../../../docs/07-encryption.md"),
    ),
    ("daemon", include_str!("../../../../docs/08-daemon.md")),
    (
        "maintenance",
        include_str!("../../../../docs/09-maintenance.md"),
    ),
    (
        "stored-procedures",
        include_str!("../../../../docs/10-stored-procedures.md"),
    ),
    (
        "extended-sql",
        include_str!("../../../../docs/11-extended-sql-functions.md"),
    ),
    (
        "operational-sql",
        include_str!("../../../../docs/12-operational-sql-commands.md"),
    ),
    (
        "triggers",
        include_str!("../../../../docs/13-triggers-and-external-table-modules.md"),
    ),
    ("auth", include_str!("../../../../docs/14-auth.md")),
    (
        "credential-enforcement",
        include_str!("../../../../docs/15-credential-enforcement.md"),
    ),
    (
        "client-conformance",
        include_str!("../../../../docs/16-client-conformance.md"),
    ),
    (
        "documentation-index",
        include_str!("../../../../docs/README.md"),
    ),
    (
        "scored-sql",
        include_str!("../../../../docs/ai/sql-scored-search.md"),
    ),
    (
        "minhash-contract",
        include_str!("../../../../docs/ai/minhash-hash-contract.md"),
    ),
    (
        "benchmark-methodology",
        include_str!("../../../../docs/ai/benchmark-methodology.md"),
    ),
];

const QUERIES: &[(&str, &str)] = &[
    ("create and open a database in Rust", "rust-quickstart"),
    ("use MongrelDB from Node.js NAPI", "nodejs-quickstart"),
    ("SELECT WHERE GROUP BY SQL syntax", "sql-queries"),
    (
        "compose bitmap range and FM native conditions",
        "native-queries",
    ),
    ("HNSW sparse MinHash index configuration", "indexes"),
    ("AES-256-GCM encrypted database keys", "encryption"),
    ("start the HTTP daemon and authenticate requests", "daemon"),
    ("backup restore compaction and retention", "maintenance"),
    ("create and execute stored procedures", "stored-procedures"),
    ("vector and set SQL functions", "extended-sql"),
    ("VACUUM ANALYZE operational SQL", "operational-sql"),
    ("triggers and external table modules", "triggers"),
    ("users roles permissions row level security masks", "auth"),
    (
        "live credential enforcement in embedded handles",
        "credential-enforcement",
    ),
    (
        "client API conformance across language bindings",
        "client-conformance",
    ),
    ("hybrid scored SQL RRF exact reranking", "scored-sql"),
    (
        "MinHash canonical scalar hashing contract",
        "minhash-contract",
    ),
    (
        "AI benchmark qualification thresholds and artifacts",
        "benchmark-methodology",
    ),
];

#[derive(Default)]
struct Metrics {
    recall: f64,
    reciprocal_rank: f64,
    ndcg: f64,
    context_coverage: usize,
    duplicates: usize,
    returned: usize,
    latency_us: Vec<u128>,
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| token.len() > 1)
        .map(str::to_ascii_lowercase)
        .collect()
}

fn token_id(token: &str) -> u32 {
    token.bytes().fold(0x811c_9dc5, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(0x0100_0193)
    })
}

fn sparse_vector(text: &str) -> Vec<(u32, f32)> {
    let mut counts = HashMap::<u32, usize>::new();
    for token in tokenize(text) {
        *counts.entry(token_id(&token)).or_default() += 1;
    }
    let mut terms = counts
        .into_iter()
        .map(|(token, count)| (token, 1.0 + (count as f32).ln()))
        .collect::<Vec<_>>();
    terms.sort_unstable_by_key(|(token, _)| *token);
    terms
}

fn dense_vector(text: &str) -> Vec<f32> {
    let mut vector = vec![0.0f32; DIM];
    for token in tokenize(text) {
        let hash = token_id(&token) as usize;
        let sign = if hash & 1 == 0 { 1.0 } else { -1.0 };
        vector[(hash >> 1) % DIM] += sign;
    }
    let norm = vector.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm > 0.0 {
        for value in &mut vector {
            *value /= norm;
        }
    }
    vector
}

fn passages() -> Vec<(&'static str, String)> {
    let mut passages = Vec::new();
    for (document, text) in DOCUMENTS {
        for section in text.split("\n## ") {
            let section = section.trim();
            if tokenize(section).len() >= 8 {
                passages.push((*document, section.to_string()));
            }
        }
    }
    passages
}

fn schema() -> Schema {
    let column = |id, name: &str, ty, primary_key| ColumnDef {
        id,
        name: name.into(),
        ty,
        flags: if primary_key {
            ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
        embedding_source: None,
    };
    let index = |name: &str, column_id, kind| IndexDef {
        name: name.into(),
        column_id,
        kind,
        predicate: None,
        options: Default::default(),
    };
    Schema {
        schema_id: 3,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "document", TypeId::Bytes, false),
            column(3, "passage", TypeId::Bytes, false),
            column(4, "embedding", TypeId::Embedding { dim: DIM as u32 }, false),
            column(5, "sparse", TypeId::Bytes, false),
        ],
        indexes: vec![
            index("embedding", 4, IndexKind::Ann),
            index("sparse", 5, IndexKind::Sparse),
        ],
        ..Schema::default()
    }
}

fn update_metrics(metrics: &mut Metrics, hits: &[u64], relevant: &HashSet<u64>, elapsed: u128) {
    let unique = hits.iter().copied().collect::<HashSet<_>>();
    metrics.duplicates += hits.len().saturating_sub(unique.len());
    metrics.returned += hits.len();
    metrics.recall += hits
        .iter()
        .filter(|row_id| relevant.contains(row_id))
        .count() as f64
        / relevant.len().max(1) as f64;
    if let Some(rank) = hits.iter().position(|row_id| relevant.contains(row_id)) {
        metrics.reciprocal_rank += 1.0 / (rank + 1) as f64;
        metrics.context_coverage += 1;
    }
    let dcg = hits
        .iter()
        .enumerate()
        .filter(|(_, row_id)| relevant.contains(row_id))
        .map(|(rank, _)| 1.0 / ((rank + 2) as f64).log2())
        .sum::<f64>();
    let ideal = (0..relevant.len().min(TOP_K))
        .map(|rank| 1.0 / ((rank + 2) as f64).log2())
        .sum::<f64>();
    metrics.ndcg += dcg / ideal.max(f64::EPSILON);
    metrics.latency_us.push(elapsed);
}

fn percentile(values: &mut [u128], percentile: f64) -> u128 {
    values.sort_unstable();
    values[((values.len() - 1) as f64 * percentile).round() as usize]
}

fn report(mut metrics: Metrics) -> serde_json::Value {
    let queries = QUERIES.len() as f64;
    serde_json::json!({
        "recall_at_10": metrics.recall / queries,
        "mrr_at_10": metrics.reciprocal_rank / queries,
        "ndcg_at_10": metrics.ndcg / queries,
        "answer_context_coverage_at_10": metrics.context_coverage as f64 / queries,
        "duplicate_suppression": 1.0 - metrics.duplicates as f64 / metrics.returned.max(1) as f64,
        "p50_us": percentile(&mut metrics.latency_us, 0.50),
        "p95_us": percentile(&mut metrics.latency_us, 0.95),
    })
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                directory_bytes(&path)
            } else {
                entry.metadata().map_or(0, |metadata| metadata.len())
            }
        })
        .sum()
}

pub fn run() -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let passages = passages();
    let mut table = Table::create(dir.path(), schema(), 3).unwrap();
    let mut relevant = HashMap::<&str, HashSet<u64>>::new();
    for (id, (document, passage)) in passages.iter().enumerate() {
        let row_id = table
            .put(vec![
                (1, Value::Int64(id as i64)),
                (2, Value::Bytes(document.as_bytes().to_vec())),
                (3, Value::Bytes(passage.as_bytes().to_vec())),
                (4, Value::Embedding(dense_vector(passage))),
                (
                    5,
                    Value::Bytes(bincode::serialize(&sparse_vector(passage)).unwrap()),
                ),
            ])
            .unwrap();
        relevant.entry(document).or_default().insert(row_id.0);
    }
    table.commit().unwrap();
    table.close().unwrap();

    let mut dense = Metrics::default();
    let mut sparse = Metrics::default();
    let mut rrf = Metrics::default();
    let mut reranked = Metrics::default();
    let candidate_k = passages.len().min(100);
    for (query, document) in QUERIES {
        let expected = &relevant[document];
        let dense_query = dense_vector(query);
        let sparse_query = sparse_vector(query);

        let started = Instant::now();
        let hits = table
            .ann_rerank(&AnnRerankRequest {
                column_id: 4,
                query: dense_query.clone(),
                candidate_k,
                limit: TOP_K,
                metric: VectorMetric::Cosine,
            })
            .unwrap()
            .into_iter()
            .map(|hit| hit.row_id.0)
            .collect::<Vec<_>>();
        update_metrics(&mut dense, &hits, expected, started.elapsed().as_micros());

        let started = Instant::now();
        let hits = table
            .retrieve(&Retriever::Sparse {
                column_id: 5,
                query: sparse_query.clone(),
                k: TOP_K,
            })
            .unwrap()
            .into_iter()
            .map(|hit| hit.row_id.0)
            .collect::<Vec<_>>();
        update_metrics(&mut sparse, &hits, expected, started.elapsed().as_micros());

        let retrievers = vec![
            NamedRetriever {
                name: "dense".into(),
                weight: 1.0,
                retriever: Retriever::Ann {
                    column_id: 4,
                    query: dense_query.clone(),
                    k: candidate_k,
                },
            },
            NamedRetriever {
                name: "sparse".into(),
                weight: 1.0,
                retriever: Retriever::Sparse {
                    column_id: 5,
                    query: sparse_query,
                    k: candidate_k,
                },
            },
        ];
        let started = Instant::now();
        let hits = table
            .search(&SearchRequest {
                must: Vec::new(),
                retrievers: retrievers.clone(),
                fusion: Fusion::ReciprocalRank { constant: 60 },
                rerank: None,
                limit: TOP_K,
                projection: None,
            })
            .unwrap()
            .into_iter()
            .map(|hit| hit.row_id.0)
            .collect::<Vec<_>>();
        update_metrics(&mut rrf, &hits, expected, started.elapsed().as_micros());

        let started = Instant::now();
        let hits = table
            .search(&SearchRequest {
                must: Vec::new(),
                retrievers,
                fusion: Fusion::ReciprocalRank { constant: 60 },
                rerank: Some(Rerank::ExactVector {
                    embedding_column: 4,
                    query: dense_query,
                    metric: VectorMetric::Cosine,
                    candidate_limit: candidate_k,
                    weight: 1.0,
                }),
                limit: TOP_K,
                projection: None,
            })
            .unwrap()
            .into_iter()
            .map(|hit| hit.row_id.0)
            .collect::<Vec<_>>();
        update_metrics(
            &mut reranked,
            &hits,
            expected,
            started.elapsed().as_micros(),
        );
    }

    serde_json::json!({
        "name": "mongreldb_technical_documentation",
        "source": "versioned repository documentation",
        "documents": DOCUMENTS.len(),
        "passages": passages.len(),
        "queries": QUERIES.len(),
        "qrels": "document-section labels",
        "index_size_bytes": directory_bytes(&dir.path().join("_idx")),
        "dense_only": report(dense),
        "sparse_only": report(sparse),
        "rrf": report(rrf),
        "rrf_exact_vector_rerank": report(reranked),
    })
}
