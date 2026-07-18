//! Manual/nightly ANN raw-candidate memory-bound characterization.

use mongreldb_core::query::{
    AiExecutionContext, Fusion, NamedRetriever, Rerank, Retriever, SearchRequest, VectorMetric,
    MAX_RAW_INDEX_CANDIDATES,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::security::CandidateAuthorization;
use mongreldb_core::{PolicyCommand, Principal, RowPolicy, SecurityCatalog, SecurityExpr, Table};
use std::time::Instant;

const DIM: usize = 8;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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
        .map(|kib| kib * 1024)
}

fn main() {
    let rows = env_usize("MONGRELDB_ANN_CAP_ROWS", MAX_RAW_INDEX_CANDIDATES + 1);
    assert!(rows > MAX_RAW_INDEX_CANDIDATES);
    let dir = tempfile::tempdir().unwrap();
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
    let mut table = Table::create(
        dir.path(),
        Schema {
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
        },
        1,
    )
    .unwrap();
    let build_started = Instant::now();
    for start in (0..rows).step_by(10_000) {
        let end = (start + 10_000).min(rows);
        table
            .put_batch(
                (start..end)
                    .map(|row| {
                        vec![
                            (1, mongreldb_core::Value::Int64(row as i64)),
                            (
                                2,
                                mongreldb_core::Value::Embedding(vec![
                                    if row == 0 {
                                        1.0
                                    } else {
                                        -1.0
                                    };
                                    DIM
                                ]),
                            ),
                            (
                                3,
                                mongreldb_core::Value::Bytes(if row == 0 {
                                    b"tenant".to_vec()
                                } else {
                                    b"other".to_vec()
                                }),
                            ),
                        ]
                    })
                    .collect(),
            )
            .unwrap();
    }
    table.commit().unwrap();
    let allowed_row_id = table
        .retrieve(&Retriever::Ann {
            column_id: 2,
            query: vec![1.0; DIM],
            k: 1,
        })
        .unwrap()[0]
        .row_id
        .0;
    let allowed_id = i64::try_from(allowed_row_id).unwrap();
    let build_ms = build_started.elapsed().as_millis();
    let security = SecurityCatalog {
        rls_tables: vec!["docs".into()],
        policies: vec![RowPolicy {
            name: "tenant_only".into(),
            table: "docs".into(),
            command: PolicyCommand::Select,
            subjects: vec!["public".into()],
            permissive: true,
            using: Some(SecurityExpr::ColumnEqValue {
                column: 1,
                value: mongreldb_core::Value::Int64(allowed_id),
            }),
            with_check: None,
        }],
        masks: Vec::new(),
    };
    let principal = Principal {
        user_id: 0,
        created_epoch: 0,
        username: "tenant".into(),
        is_admin: false,
        roles: Vec::new(),
        permissions: Vec::new(),
    };
    let authorization = CandidateAuthorization {
        table: "docs",
        security: &security,
        principal: &principal,
    };
    let request = SearchRequest {
        must: Vec::new(),
        retrievers: vec![NamedRetriever {
            name: "dense".into(),
            weight: 1.0,
            retriever: Retriever::Ann {
                column_id: 2,
                query: vec![1.0; DIM],
                k: 5,
            },
        }],
        fusion: Fusion::ReciprocalRank { constant: 60 },
        rerank: Some(Rerank::ExactVector {
            embedding_column: 2,
            query: vec![1.0; DIM],
            metric: VectorMetric::Cosine,
            candidate_limit: 5,
            weight: 1.0,
        }),
        limit: 5,
        projection: Some(vec![1]),
    };
    let context = AiExecutionContext::with_limits(
        std::time::Duration::from_secs(300),
        usize::MAX,
        MAX_RAW_INDEX_CANDIDATES,
    );
    let query_started = Instant::now();
    let (hits, trace) = mongreldb_core::trace::QueryTrace::capture(|| {
        table.search_at_with_candidate_authorization_and_context(
            &request,
            table.snapshot(),
            Some(&authorization),
            Some(&context),
        )
    });
    let hits = hits.unwrap();
    println!(
        "{}",
        serde_json::json!({
            "profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "rows": rows,
            "raw_candidate_cap": MAX_RAW_INDEX_CANDIDATES,
            "rls_selectivity": 1.0 / rows as f64,
            "build_ms": build_ms,
            "query_ms": query_started.elapsed().as_millis(),
            "peak_rss_bytes": peak_rss_bytes(),
            "hits": hits.len(),
            "available_authorized_hit_returned": hits.iter().any(|hit| hit.cells.iter().any(|cell| cell.1 == mongreldb_core::Value::Int64(allowed_id))),
            "exact_rerank_applied": hits.iter().all(|hit| hit.exact_rerank_score.is_some()),
            "ann_candidate_cap_hit": trace.ann_candidate_cap_hit,
            "rls_rows_evaluated": trace.rls_rows_evaluated,
            "work_consumed": context.consumed_work(),
            "oom_or_failure": false,
        })
    );
}
