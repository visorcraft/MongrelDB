//! Same-table scored-reader/write concurrency characterization.

use mongreldb_core::query::{AiExecutionContext, AnnRerankRequest, VectorMetric};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{
    ColumnOperation, Database, Permission, PolicyCommand, Principal, ReadAuthorization, RowPolicy,
    SecurityCatalog, SecurityExpr, Value,
};
use std::sync::{Arc, Barrier};
use std::time::Instant;

const DIM: usize = 64;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn embedding(id: usize) -> Vec<f32> {
    (0..DIM)
        .map(|d| {
            let v = (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
                ^ (d as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            ((v >> 40) as f32 / (1u32 << 24) as f32) * 2.0 - 1.0
        })
        .collect()
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
    };
    Schema {
        schema_id: 1,
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: DIM as u32 }, false),
            column(3, "owner", TypeId::Bytes, false),
        ],
        indexes: vec![IndexDef {
            name: "embedding".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        }],
        ..Schema::default()
    }
}

fn percentile(values: &mut [u128], fraction: f64) -> Option<u128> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(values[((values.len() - 1) as f64 * fraction).round() as usize])
}

fn latency_report(mut values: Vec<u128>) -> serde_json::Value {
    serde_json::json!({
        "count": values.len(),
        "p50_us": percentile(&mut values, 0.50),
        "p95_us": percentile(&mut values, 0.95),
        "p99_us": percentile(&mut values, 0.99),
    })
}

fn peak_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kb| kb * 1024)
}

fn main() {
    let rows = env_usize("MONGRELDB_AI_CONCURRENCY_ROWS", 10_000);
    let operations = env_usize("MONGRELDB_AI_CONCURRENCY_OPS", 25);
    let dir = tempfile::tempdir().unwrap();
    let database = Arc::new(Database::create(dir.path()).unwrap());
    database.create_table("docs", schema()).unwrap();
    {
        let handle = database.table("docs").unwrap();
        let mut table = handle.lock();
        for id in 0..rows {
            table
                .put(vec![
                    (1, Value::Int64(id as i64)),
                    (2, Value::Embedding(embedding(id))),
                    (3, Value::Bytes(b"tenant".to_vec())),
                ])
                .unwrap();
        }
        table.commit().unwrap();
    }
    database
        .set_security_catalog(SecurityCatalog {
            rls_tables: vec!["docs".into()],
            policies: vec![RowPolicy {
                name: "owner".into(),
                table: "docs".into(),
                command: PolicyCommand::Select,
                subjects: vec!["public".into()],
                permissive: true,
                using: Some(SecurityExpr::ColumnEqCurrentUser { column: 3 }),
                with_check: None,
            }],
            masks: Vec::new(),
        })
        .unwrap();
    let principal = Principal {
        username: "tenant".into(),
        is_admin: false,
        roles: Vec::new(),
        permissions: vec![Permission::Select {
            table: "docs".into(),
        }],
    };
    let authorization = ReadAuthorization {
        operation: ColumnOperation::Select,
        columns: vec![2],
        permissions: Vec::new(),
    };
    let request = AnnRerankRequest {
        column_id: 2,
        query: embedding(0),
        candidate_k: rows.min(100),
        limit: 10,
        metric: VectorMetric::Cosine,
    };
    let next_id = Arc::new(std::sync::atomic::AtomicUsize::new(rows));
    let mut scenarios = Vec::new();
    for readers in [1usize, 4, 16, 32] {
        for writers in [0usize, 1, 4] {
            let barrier = Arc::new(Barrier::new(readers + writers + 1));
            let reader_threads = (0..readers)
                .map(|_| {
                    let database = Arc::clone(&database);
                    let barrier = Arc::clone(&barrier);
                    let principal = principal.clone();
                    let authorization = authorization.clone();
                    let request = request.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        (0..operations)
                            .map(|_| {
                                let started = Instant::now();
                                let context = AiExecutionContext::with_timeout(
                                    std::time::Duration::from_secs(30),
                                    1_000_000,
                                );
                                database
                                    .with_authorized_scored_read_context_at(
                                        "docs",
                                        Some(&principal),
                                        false,
                                        Some(&authorization),
                                        Some(&context),
                                        None,
                                        |table, snapshot, candidate_authorization, _| {
                                            table
                                                .ann_rerank_at_with_candidate_authorization_on_generation(
                                                    &request,
                                                    snapshot,
                                                    candidate_authorization,
                                                    Some(&context),
                                                )
                                                .map(|_| ())
                                        },
                                    )
                                    .unwrap();
                                started.elapsed().as_micros()
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            let writer_threads = (0..writers)
                .map(|_| {
                    let database = Arc::clone(&database);
                    let barrier = Arc::clone(&barrier);
                    let next_id = Arc::clone(&next_id);
                    std::thread::spawn(move || {
                        barrier.wait();
                        (0..operations)
                            .map(|_| {
                                let id = next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let started = Instant::now();
                                let handle = database.table("docs").unwrap();
                                let mut table = handle.lock();
                                table
                                    .put(vec![
                                        (1, Value::Int64(id as i64)),
                                        (2, Value::Embedding(embedding(id))),
                                        (3, Value::Bytes(b"tenant".to_vec())),
                                    ])
                                    .unwrap();
                                table.commit().unwrap();
                                started.elapsed().as_micros()
                            })
                            .collect::<Vec<_>>()
                    })
                })
                .collect::<Vec<_>>();
            let started = Instant::now();
            barrier.wait();
            let query_latency = reader_threads
                .into_iter()
                .flat_map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>();
            let commit_latency = writer_threads
                .into_iter()
                .flat_map(|thread| thread.join().unwrap())
                .collect::<Vec<_>>();
            let elapsed = started.elapsed();
            let total = query_latency.len() + commit_latency.len();
            scenarios.push(serde_json::json!({
                "readers": readers,
                "writers": writers,
                "query_latency": latency_report(query_latency),
                "commit_latency": latency_report(commit_latency),
                "throughput_ops_per_second": total as f64 / elapsed.as_secs_f64(),
                "elapsed_ms": elapsed.as_millis(),
                "peak_rss_bytes": peak_rss_bytes(),
            }));
        }
    }
    println!(
        "{}",
        serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "operations_per_worker": operations,
            "rls": true,
            "exact_vector_rerank": true,
            "scenarios": scenarios,
        })
    );
}
