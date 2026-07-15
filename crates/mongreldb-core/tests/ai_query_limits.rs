use mongreldb_core::query::{
    AiExecutionContext, AnnRerankRequest, Condition, Fusion, NamedRetriever, Retriever,
    SearchRequest, SetMember, SetSimilarityRequest, VectorMetric, MAX_FINAL_LIMIT,
    MAX_FUSED_CANDIDATES, MAX_PROJECTION_COLUMNS, MAX_RETRIEVERS, MAX_RETRIEVER_K, MAX_SET_MEMBERS,
    MAX_SPARSE_TERMS,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use mongreldb_core::{Database, Epoch, MongrelError, Snapshot, Table, TableHandle, Value};

fn table() -> (tempfile::TempDir, Table) {
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
    };
    let schema = Schema {
        columns: vec![
            column(1, "id", TypeId::Int64, true),
            column(2, "embedding", TypeId::Embedding { dim: 8 }, false),
            column(3, "sparse", TypeId::Bytes, false),
            column(4, "members", TypeId::Bytes, false),
        ],
        indexes: vec![
            IndexDef {
                name: "ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "sparse".into(),
                column_id: 3,
                kind: IndexKind::Sparse,
                predicate: None,
                options: Default::default(),
            },
            IndexDef {
                name: "minhash".into(),
                column_id: 4,
                kind: IndexKind::MinHash,
                predicate: None,
                options: Default::default(),
            },
        ],
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(vec![1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(1u32, f32::MAX)]).unwrap()),
            ),
            (4, Value::Bytes(serde_json::to_vec(&vec!["a"]).unwrap())),
        ])
        .unwrap();
    table.commit().unwrap();
    (dir, table)
}

fn ann(k: usize) -> Retriever {
    Retriever::Ann {
        column_id: 2,
        query: vec![1.0; 8],
        k,
    }
}

fn search(retrievers: Vec<NamedRetriever>, limit: usize) -> SearchRequest {
    SearchRequest {
        must: vec![],
        retrievers,
        fusion: Fusion::ReciprocalRank { constant: 60 },
        rerank: None,
        limit,
        projection: Some(vec![1]),
    }
}

fn named(name: String, retriever: Retriever) -> NamedRetriever {
    NamedRetriever {
        name,
        weight: 1.0,
        retriever,
    }
}

#[test]
fn public_ai_cardinalities_fail_closed() {
    let (_dir, mut table) = table();
    assert!(matches!(
        table.query(&mongreldb_core::Query::new().with_limit(usize::MAX)),
        Err(MongrelError::InvalidArgument(_))
    ));
    let error = table
        .search(&search(vec![named("ann".into(), ann(1))], usize::MAX))
        .unwrap_err();
    assert!(matches!(error, MongrelError::InvalidArgument(_)));

    assert!(matches!(
        table.retrieve(&ann(MAX_RETRIEVER_K + 1)),
        Err(MongrelError::InvalidArgument(_))
    ));

    let too_many = (0..=MAX_RETRIEVERS)
        .map(|index| named(format!("ann{index}"), ann(1)))
        .collect();
    assert!(matches!(
        table.search(&search(too_many, 1)),
        Err(MongrelError::InvalidArgument(_))
    ));

    assert!(matches!(
        table.retrieve(&Retriever::Sparse {
            column_id: 3,
            query: vec![(1, 1.0); MAX_SPARSE_TERMS + 1],
            k: 1,
        }),
        Err(MongrelError::InvalidArgument(_))
    ));
    assert!(matches!(
        table.retrieve(&Retriever::MinHash {
            column_id: 4,
            members: vec![SetMember::Boolean(true); MAX_SET_MEMBERS + 1],
            k: 1,
        }),
        Err(MongrelError::InvalidArgument(_))
    ));
    assert!(matches!(
        table.retrieve(&Retriever::MinHash {
            column_id: 4,
            members: vec![SetMember::String(
                "x".repeat(mongreldb_core::query::MAX_SET_MEMBER_BYTES + 1)
            )],
            k: 1,
        }),
        Err(MongrelError::InvalidArgument(_))
    ));

    let mut request = search(vec![named("ann".into(), ann(1))], 1);
    request.projection = Some(vec![1; MAX_PROJECTION_COLUMNS + 1]);
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));

    let mut request = search(
        vec![named(
            "x".repeat(mongreldb_core::query::MAX_RETRIEVER_NAME_BYTES + 1),
            ann(1),
        )],
        1,
    );
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));

    request = search(vec![named("ann".into(), ann(1))], 1);
    request.must.push(Condition::BitmapIn {
        column_id: 1,
        values: vec![Vec::new(); MAX_SET_MEMBERS + 1],
    });
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));

    let mut request = search(vec![named("ann".into(), ann(1))], 1);
    request.must.push(Condition::Ann {
        column_id: 2,
        query: vec![1.0; 8],
        k: 1,
    });
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));
}

#[test]
fn native_query_offset_applies_before_limit() {
    let (_dir, mut table) = table();
    for id in 2..=4 {
        table
            .put(vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(vec![1.0; 8])),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(id as u32, 1.0)]).unwrap()),
                ),
                (4, Value::Bytes(b"[]".to_vec())),
            ])
            .unwrap();
    }
    table.commit().unwrap();

    let rows = table
        .query(&mongreldb_core::Query::new().with_offset(2).with_limit(1))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].columns[&1], Value::Int64(3));

    let range = Condition::Range {
        column_id: 1,
        lo: 1,
        hi: 4,
    };
    let first_page = table
        .query_cached(
            &mongreldb_core::Query::new()
                .and(range.clone())
                .with_limit(1),
        )
        .unwrap();
    let third_page = table
        .query_cached(
            &mongreldb_core::Query::new()
                .and(range)
                .with_offset(2)
                .with_limit(1),
        )
        .unwrap();
    assert_eq!(first_page[0].columns[&1], Value::Int64(1));
    assert_eq!(third_page[0].columns[&1], Value::Int64(3));
}

#[test]
fn ai_scores_remain_finite_or_return_typed_errors() {
    let (_dir, mut table) = table();
    let sparse = table
        .retrieve(&Retriever::Sparse {
            column_id: 3,
            query: vec![(1, f32::MAX)],
            k: 1,
        })
        .unwrap();
    let mongreldb_core::query::RetrieverScore::SparseDotProduct(score) = sparse[0].score else {
        panic!("expected sparse score")
    };
    assert!(score.is_finite());

    let mut request = search(vec![named("ann".into(), ann(1))], MAX_FINAL_LIMIT);
    request.retrievers[0].weight = f64::MAX;
    assert!(matches!(
        table.search(&request),
        Err(MongrelError::InvalidArgument(_))
    ));

    let hits = table
        .ann_rerank(&AnnRerankRequest {
            column_id: 2,
            query: vec![1.0; 8],
            candidate_k: 1,
            limit: 1,
            metric: VectorMetric::Cosine,
        })
        .unwrap();
    assert!(hits[0].exact_score.is_finite());
}

#[test]
fn actual_work_budget_and_cancellation_fail_with_typed_errors() {
    let (_dir, mut table) = table();
    let snapshot = table.snapshot();
    let sparse = Retriever::Sparse {
        column_id: 3,
        query: vec![(1, 1.0)],
        k: 1,
    };

    let exhausted = AiExecutionContext::new(None, 0);
    assert!(matches!(
        table.retrieve_at_with_candidate_authorization_and_context(
            &sparse,
            snapshot,
            None,
            Some(&exhausted),
        ),
        Err(MongrelError::WorkBudgetExceeded)
    ));

    let cancelled = AiExecutionContext::new(None, usize::MAX);
    cancelled.cancel();
    assert!(matches!(
        table.search_at_with_candidate_authorization_and_context(
            &search(vec![named("sparse".into(), sparse.clone())], 1),
            snapshot,
            None,
            Some(&cancelled),
        ),
        Err(MongrelError::Cancelled)
    ));
    assert_eq!(table.retrieve(&sparse).unwrap().len(), 1);

    let tiny_minhash = AiExecutionContext::new(None, 1);
    assert!(matches!(
        table.retrieve_at_with_candidate_authorization_and_context(
            &Retriever::MinHash {
                column_id: 4,
                members: vec![SetMember::String("a".into())],
                k: 1,
            },
            snapshot,
            None,
            Some(&tiny_minhash),
        ),
        Err(MongrelError::WorkBudgetExceeded)
    ));
}

fn ann_table(dim: u32) -> (tempfile::TempDir, Table) {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema {
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim },
                flags: ColumnFlags::empty(),
                default_value: None,
            },
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
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(vec![1.0; dim as usize])),
        ])
        .unwrap();
    table.commit().unwrap();
    (dir, table)
}

#[test]
fn exact_rerank_work_scales_with_embedding_width() {
    let (_narrow_dir, mut narrow) = ann_table(128);
    let narrow_context = AiExecutionContext::new(None, usize::MAX);
    narrow
        .ann_rerank_at_with_context(
            &AnnRerankRequest {
                column_id: 2,
                query: vec![1.0; 128],
                candidate_k: 1,
                limit: 1,
                metric: VectorMetric::Cosine,
            },
            narrow.snapshot(),
            None,
            Some(&narrow_context),
        )
        .unwrap();

    let (_wide_dir, mut wide) = ann_table(65_536);
    let wide_context = AiExecutionContext::new(None, usize::MAX);
    wide.ann_rerank_at_with_context(
        &AnnRerankRequest {
            column_id: 2,
            query: vec![1.0; 65_536],
            candidate_k: 1,
            limit: 1,
            metric: VectorMetric::Cosine,
        },
        wide.snapshot(),
        None,
        Some(&wide_context),
    )
    .unwrap();

    assert!(wide_context.consumed_work() > narrow_context.consumed_work());
}

fn set_table(member_count: usize) -> (tempfile::TempDir, Table) {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema {
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
            },
            ColumnDef {
                id: 2,
                name: "members".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "minhash".into(),
            column_id: 2,
            kind: IndexKind::MinHash,
            predicate: None,
            options: Default::default(),
        }],
        ..Schema::default()
    };
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table
        .put(vec![
            (1, Value::Int64(1)),
            (
                2,
                Value::Bytes(serde_json::to_vec(&vec![true; member_count]).unwrap()),
            ),
        ])
        .unwrap();
    table.commit().unwrap();
    (dir, table)
}

#[test]
fn exact_set_work_scales_with_stored_member_count() {
    let request = SetSimilarityRequest {
        column_id: 2,
        members: vec![SetMember::Boolean(true)],
        candidate_k: 1,
        limit: 1,
        min_jaccard: 0.0,
    };
    let (_small_dir, mut small) = set_table(1);
    let small_context = AiExecutionContext::new(None, usize::MAX);
    small
        .set_similarity_at_with_context(&request, small.snapshot(), None, Some(&small_context))
        .unwrap();

    let (_large_dir, mut large) = set_table(MAX_SET_MEMBERS);
    let large_context = AiExecutionContext::new(None, usize::MAX);
    large
        .set_similarity_at_with_context(&request, large.snapshot(), None, Some(&large_context))
        .unwrap();

    assert!(large_context.consumed_work() > small_context.consumed_work());
}

#[test]
fn fused_union_ceiling_projection_charging_and_zero_weight_are_enforced() {
    let (_dir, mut table) = table();
    table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Embedding(vec![-1.0; 8])),
            (
                3,
                Value::Bytes(bincode::serialize(&vec![(2u32, 1.0f32)]).unwrap()),
            ),
            (4, Value::Bytes(serde_json::to_vec(&vec!["b"]).unwrap())),
        ])
        .unwrap();
    table.commit().unwrap();
    let snapshot = table.snapshot();
    let sparse = |term| Retriever::Sparse {
        column_id: 3,
        query: vec![(term, 1.0)],
        k: 1,
    };

    let ceiling =
        AiExecutionContext::with_limits(std::time::Duration::from_secs(30), usize::MAX, 1);
    assert!(matches!(
        table.search_at_with_candidate_authorization_and_context(
            &search(
                vec![
                    named("first".into(), sparse(1)),
                    named("second".into(), sparse(2)),
                ],
                1,
            ),
            snapshot,
            None,
            Some(&ceiling),
        ),
        Err(MongrelError::WorkBudgetExceeded)
    ));

    let narrow_context = AiExecutionContext::with_limits(
        std::time::Duration::from_secs(30),
        usize::MAX,
        MAX_FUSED_CANDIDATES,
    );
    table
        .search_at_with_candidate_authorization_and_context(
            &search(vec![named("sparse".into(), sparse(1))], 1),
            snapshot,
            None,
            Some(&narrow_context),
        )
        .unwrap();
    let mut full_projection = search(vec![named("sparse".into(), sparse(1))], 1);
    full_projection.projection = None;
    let full_context = AiExecutionContext::with_limits(
        std::time::Duration::from_secs(30),
        usize::MAX,
        MAX_FUSED_CANDIDATES,
    );
    table
        .search_at_with_candidate_authorization_and_context(
            &full_projection,
            snapshot,
            None,
            Some(&full_context),
        )
        .unwrap();
    assert!(full_context.consumed_work() > narrow_context.consumed_work());

    let mut skipped = named("disabled".into(), ann(1));
    skipped.weight = 0.0;
    let zero_context = AiExecutionContext::new(None, 0);
    assert!(table
        .search_at_with_candidate_authorization_and_context(
            &search(vec![skipped], 1),
            snapshot,
            None,
            Some(&zero_context),
        )
        .unwrap()
        .is_empty());
    assert_eq!(zero_context.consumed_work(), 0);
}

#[test]
fn ann_adaptive_overfetch_stops_at_context_cap_and_reports_exhaustion() {
    let (_dir, mut table) = table();
    for id in 2..=20 {
        table
            .put(vec![
                (1, Value::Int64(id)),
                (2, Value::Embedding(vec![-1.0; 8])),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(id as u32, 1.0f32)]).unwrap()),
                ),
                (4, Value::Bytes(serde_json::to_vec(&vec![id]).unwrap())),
            ])
            .unwrap();
    }
    table.commit().unwrap();
    let mut request = search(vec![named("ann".into(), ann(5))], 5);
    request.must = vec![Condition::Pk(Value::Int64(1).encode_key())];
    let context =
        AiExecutionContext::with_limits(std::time::Duration::from_secs(30), usize::MAX, 4);
    let snapshot = table.snapshot();
    let (hits, trace) = mongreldb_core::trace::QueryTrace::capture(|| {
        table.search_at_with_candidate_authorization_and_context(
            &request,
            snapshot,
            None,
            Some(&context),
        )
    });
    let hits = hits.unwrap();
    assert_eq!(hits.len(), 1);
    assert!(trace.ann_candidate_cap_hit);
    assert!(context.consumed_work() < usize::MAX);
}

#[test]
fn authorized_lock_wait_observes_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    database
        .create_table(
            "docs",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    let handle = database.table("docs").unwrap();
    let guard = handle.lock();
    let context = AiExecutionContext::with_timeout(std::time::Duration::from_millis(10), 100);
    let started = std::time::Instant::now();
    let result = database.with_authorized_scored_read_context_at(
        "docs",
        None,
        false,
        None,
        Some(&context),
        None,
        |_, _, _, _| Ok(()),
    );
    assert!(matches!(result, Err(MongrelError::DeadlineExceeded)));
    assert!(started.elapsed() < std::time::Duration::from_millis(100));
    drop(guard);

    database
        .with_authorized_scored_read_context_at(
            "docs",
            None,
            false,
            None,
            None,
            None,
            |_, _, _, _| Ok(()),
        )
        .unwrap();
}

#[test]
fn scored_read_generation_does_not_hold_write_lock() {
    let dir = tempfile::tempdir().unwrap();
    let database = std::sync::Arc::new(Database::create(dir.path()).unwrap());
    database
        .create_table(
            "docs",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let reader = {
        let database = std::sync::Arc::clone(&database);
        let entered = std::sync::Arc::clone(&entered);
        let release = std::sync::Arc::clone(&release);
        std::thread::spawn(move || {
            database.with_authorized_scored_read_context_at(
                "docs",
                None,
                false,
                None,
                None,
                None,
                |_, _, _, _| {
                    entered.wait();
                    release.wait();
                    Ok(())
                },
            )
        })
    };
    entered.wait();

    let (sent, received) = std::sync::mpsc::channel();
    let writer = {
        let handle = database.table("docs").unwrap();
        std::thread::spawn(move || {
            let result = (|| {
                let mut table = handle.lock();
                table.put(vec![(1, Value::Int64(1))])?;
                table.commit()?;
                Ok::<_, MongrelError>(())
            })();
            sent.send(result).unwrap();
        })
    };
    let write_result = received.recv_timeout(std::time::Duration::from_millis(500));
    release.wait();
    reader.join().unwrap().unwrap();
    writer.join().unwrap();
    write_result
        .expect("same-table write blocked by scored read generation")
        .unwrap();
}

#[test]
fn scored_read_generation_pins_run_files_across_compaction_gc() {
    let dir = tempfile::tempdir().unwrap();
    let database = std::sync::Arc::new(Database::create(dir.path()).unwrap());
    database
        .create_table(
            "docs",
            Schema {
                columns: vec![
                    ColumnDef {
                        id: 1,
                        name: "id".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                        default_value: None,
                    },
                    ColumnDef {
                        id: 2,
                        name: "sparse".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                    },
                ],
                indexes: vec![IndexDef {
                    name: "sparse".into(),
                    column_id: 2,
                    kind: IndexKind::Sparse,
                    predicate: None,
                    options: Default::default(),
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    for id in 1..=2 {
        database
            .transaction(|transaction| {
                transaction.put(
                    "docs",
                    vec![
                        (1, Value::Int64(id)),
                        (
                            2,
                            Value::Bytes(bincode::serialize(&vec![(1u32, id as f32)]).unwrap()),
                        ),
                    ],
                )?;
                Ok(())
            })
            .unwrap();
        database
            .table("docs")
            .unwrap()
            .lock()
            .force_flush()
            .unwrap();
    }
    assert_eq!(database.table("docs").unwrap().lock().run_count(), 2);

    let entered = std::sync::Arc::new(std::sync::Barrier::new(2));
    let release = std::sync::Arc::new(std::sync::Barrier::new(2));
    let reader = {
        let database = std::sync::Arc::clone(&database);
        let entered = std::sync::Arc::clone(&entered);
        let release = std::sync::Arc::clone(&release);
        std::thread::spawn(move || {
            database.with_authorized_scored_read_context_at(
                "docs",
                None,
                false,
                None,
                None,
                None,
                |table, snapshot, authorization, _| {
                    entered.wait();
                    release.wait();
                    table.retrieve_at_with_candidate_authorization_on_generation(
                        &Retriever::Sparse {
                            column_id: 2,
                            query: vec![(1, 1.0)],
                            k: 2,
                        },
                        snapshot,
                        authorization,
                        None,
                    )
                },
            )
        })
    };
    entered.wait();
    assert!(database.compact_table("docs").unwrap());
    database.gc().unwrap();
    release.wait();
    assert_eq!(reader.join().unwrap().unwrap().len(), 2);

    assert!(database.gc().unwrap() >= 2);
    let hits = database
        .with_authorized_scored_read_context_at(
            "docs",
            None,
            false,
            None,
            None,
            None,
            |table, snapshot, authorization, _| {
                table.retrieve_at_with_candidate_authorization_on_generation(
                    &Retriever::Sparse {
                        column_id: 2,
                        query: vec![(1, 1.0)],
                        k: 2,
                    },
                    snapshot,
                    authorization,
                    None,
                )
            },
        )
        .unwrap();
    assert_eq!(hits.len(), 2);
}

#[test]
fn read_generations_are_immutable_shared_and_cow_free() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::create(dir.path()).unwrap();
    database
        .create_table(
            "docs",
            Schema {
                columns: vec![ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                }],
                ..Schema::default()
            },
        )
        .unwrap();
    let handle = database.table("docs").unwrap();
    let (first, _) = handle.read_generation_with_context(None).unwrap();
    let (second, _) = handle.read_generation_with_context(None).unwrap();
    assert_eq!(handle.generation_stats().active_read_generations, 2);

    {
        let mut writer = handle.lock();
        writer.put(vec![(1, Value::Int64(1))]).unwrap();
        writer.commit().unwrap();
        assert_eq!(writer.count(), 1);
    }

    assert_eq!(first.count(), 0);
    assert_eq!(second.count(), 0);
    let stats = handle.generation_stats();
    assert_eq!(stats.cow_clone_count, 0);
    assert_eq!(stats.estimated_cow_clone_bytes, 0);
    assert_eq!(stats.max_live_read_generations, 2);
    drop((first, second));
    assert_eq!(handle.generation_stats().active_read_generations, 0);
}

#[test]
fn active_generation_keeps_ann_sparse_and_minhash_deltas_private() {
    let (_dir, table) = table();
    let handle = TableHandle::from_table(table);
    let (generation, _) = handle.read_generation_with_context(None).unwrap();
    {
        let mut writer = handle.lock();
        writer
            .put(vec![
                (1, Value::Int64(2)),
                (2, Value::Embedding(vec![1.0; 8])),
                (
                    3,
                    Value::Bytes(bincode::serialize(&vec![(1u32, f32::MAX)]).unwrap()),
                ),
                (4, Value::Bytes(serde_json::to_vec(&vec!["a"]).unwrap())),
            ])
            .unwrap();
        writer.commit().unwrap();
    }

    let retrievers = [
        ann(10),
        Retriever::Sparse {
            column_id: 3,
            query: vec![(1, 1.0)],
            k: 10,
        },
        Retriever::MinHash {
            column_id: 4,
            members: vec![SetMember::String("a".into())],
            k: 10,
        },
    ];
    for retriever in retrievers {
        let old = generation
            .retrieve_at_with_candidate_authorization_on_generation(
                &retriever,
                Snapshot::at(Epoch(u64::MAX)),
                None,
                None,
            )
            .unwrap();
        let current = handle
            .lock()
            .retrieve_with_allowed(&retriever, None)
            .unwrap();
        assert_eq!(old.len(), 1);
        assert_eq!(current.len(), 2);
    }
    assert_eq!(handle.generation_stats().cow_clone_count, 0);
}
