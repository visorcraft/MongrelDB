use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{
    CancellationReason, Database, EmbeddingError, EmbeddingFailurePolicy, EmbeddingNormalization,
    EmbeddingProvider, EmbeddingRequest, EmbeddingResponse, EmbeddingSource, ExecutionControl,
    GeneratedEmbeddingSpec, Permission, PolicyCommand, RowPolicy, SecurityCatalog, SecurityExpr,
    StoredTrigger, TriggerCell, TriggerDefinition, TriggerEvent, TriggerProgram, TriggerStep,
    TriggerTarget, TriggerTiming, TriggerValue, Value,
};
use tempfile::tempdir;

#[derive(Clone, Copy)]
enum ProviderOutput {
    Valid,
    Failed,
    Empty,
    WrongDimension,
    NonFinite,
}

struct TextProvider {
    calls: Arc<AtomicUsize>,
    output: ProviderOutput,
    model_version: &'static str,
}

impl TextProvider {
    fn valid() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            output: ProviderOutput::Valid,
            model_version: "1",
        }
    }

    fn with_output(calls: Arc<AtomicUsize>, output: ProviderOutput) -> Self {
        Self {
            calls,
            output,
            model_version: "1",
        }
    }
}

impl EmbeddingProvider for TextProvider {
    fn provider_id(&self) -> &str {
        "text-test"
    }

    fn model_id(&self) -> &str {
        "length-and-sum"
    }

    fn model_version(&self) -> &str {
        self.model_version
    }

    fn dimension(&self) -> u32 {
        2
    }

    fn normalization(&self) -> EmbeddingNormalization {
        EmbeddingNormalization::None
    }

    fn preprocessing_version(&self) -> &str {
        "raw-utf8-v1"
    }

    fn embed(&self, request: EmbeddingRequest<'_>) -> Result<EmbeddingResponse, EmbeddingError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.output {
            ProviderOutput::Valid => Ok(EmbeddingResponse {
                vectors: request
                    .texts
                    .iter()
                    .map(|text| {
                        vec![
                            text.len() as f32,
                            text.bytes().map(u32::from).sum::<u32>() as f32,
                        ]
                    })
                    .collect(),
            }),
            ProviderOutput::Failed => Err(EmbeddingError::ProviderFailed {
                provider: self.provider_id().into(),
                message: "injected failure".into(),
            }),
            ProviderOutput::Empty => Ok(EmbeddingResponse {
                vectors: Vec::new(),
            }),
            ProviderOutput::WrongDimension => Ok(EmbeddingResponse {
                vectors: vec![vec![1.0]],
            }),
            ProviderOutput::NonFinite => Ok(EmbeddingResponse {
                vectors: vec![vec![f32::NAN, 0.0]],
            }),
        }
    }
}

fn schema() -> Schema {
    Schema {
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 2 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: Some(EmbeddingSource::GeneratedColumn {
                    spec: GeneratedEmbeddingSpec {
                        provider_id: "text-test".into(),
                        model_id: "length-and-sum".into(),
                        model_version: "1".into(),
                        source_columns: vec![2],
                        input_template: "{text}".into(),
                        dimension: 2,
                        normalization: EmbeddingNormalization::None,
                        failure_policy: EmbeddingFailurePolicy::AbortWrite,
                    },
                }),
            },
        ],
        ..Schema::default()
    }
}

#[test]
fn insert_and_update_materialize_generated_embeddings() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.embedding_providers()
        .register_new(Arc::new(TextProvider::valid()))
        .unwrap();
    db.create_table("documents", schema()).unwrap();

    let mut insert = db.begin();
    insert
        .put(
            "documents",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"cat".to_vec()))],
        )
        .unwrap();
    insert.commit().unwrap();

    let row = db.rows_for("documents", None).unwrap().remove(0);
    assert_eq!(
        row.columns.get(&3),
        Some(&Value::Embedding(vec![3.0, 312.0]))
    );

    let mut update = db.begin();
    update
        .update_many(
            "documents",
            vec![(row.row_id, vec![(2, Value::Bytes(b"horse".to_vec()))])],
        )
        .unwrap();
    update.commit().unwrap();

    let row = db.rows_for("documents", None).unwrap().remove(0);
    assert_eq!(
        row.columns.get(&3),
        Some(&Value::Embedding(vec![5.0, 545.0]))
    );
}

#[test]
fn provider_failures_and_invalid_outputs_roll_back_source_rows() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("documents", schema()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut generation = db
        .embedding_providers()
        .register_new(Arc::new(TextProvider::with_output(
            Arc::clone(&calls),
            ProviderOutput::Failed,
        )))
        .unwrap();

    for (id, output) in [
        (1, ProviderOutput::Failed),
        (2, ProviderOutput::Empty),
        (3, ProviderOutput::WrongDimension),
        (4, ProviderOutput::NonFinite),
    ] {
        if id != 1 {
            generation = db
                .embedding_providers()
                .replace(
                    generation,
                    Arc::new(TextProvider::with_output(Arc::clone(&calls), output)),
                )
                .unwrap();
        }
        let mut transaction = db.begin();
        transaction
            .put(
                "documents",
                vec![(1, Value::Int64(id)), (2, Value::Bytes(b"secret".to_vec()))],
            )
            .unwrap();
        assert!(transaction.commit().is_err());
        assert!(db.rows_for("documents", None).unwrap().is_empty());
    }
    assert_eq!(calls.load(Ordering::Relaxed), 4);

    drop(db);
    let reopened = Database::open(dir.path()).unwrap();
    assert!(reopened.rows_for("documents", None).unwrap().is_empty());
}

#[test]
fn model_version_cancellation_and_deadline_abort_before_wal() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.create_table("documents", schema()).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let generation = db
        .embedding_providers()
        .register_new(Arc::new(TextProvider {
            calls: Arc::clone(&calls),
            output: ProviderOutput::Valid,
            model_version: "2",
        }))
        .unwrap();

    let mut transaction = db.begin();
    transaction
        .put(
            "documents",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"one".to_vec()))],
        )
        .unwrap();
    assert!(transaction.commit().is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 0);

    db.embedding_providers()
        .replace(
            generation,
            Arc::new(TextProvider::with_output(
                Arc::clone(&calls),
                ProviderOutput::Valid,
            )),
        )
        .unwrap();

    for (id, control) in [
        {
            let control = ExecutionControl::new(None);
            control.cancel(CancellationReason::ClientRequest);
            (2, control)
        },
        (3, ExecutionControl::with_timeout(Duration::ZERO)),
    ] {
        let mut transaction = db.begin();
        transaction
            .put(
                "documents",
                vec![(1, Value::Int64(id)), (2, Value::Bytes(b"two".to_vec()))],
            )
            .unwrap();
        assert!(transaction.commit_controlled(&control, || Ok(())).is_err());
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(db.rows_for("documents", None).unwrap().is_empty());
}

#[test]
fn triggers_run_before_generated_embedding_materialization() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.embedding_providers()
        .register_new(Arc::new(TextProvider::valid()))
        .unwrap();
    db.create_table("documents", schema()).unwrap();
    db.create_trigger(
        StoredTrigger::new(
            "replace_text",
            TriggerDefinition {
                target: TriggerTarget::Table("documents".into()),
                timing: TriggerTiming::Before,
                event: TriggerEvent::Insert,
                update_of: Vec::new(),
                target_columns: Vec::new(),
                when: None,
                program: TriggerProgram {
                    steps: vec![TriggerStep::SetNew {
                        cells: vec![TriggerCell {
                            column_id: 2,
                            value: TriggerValue::Literal(Value::Bytes(b"dog".to_vec())),
                        }],
                    }],
                },
            },
            0,
        )
        .unwrap(),
    )
    .unwrap();

    let mut transaction = db.begin();
    transaction
        .put(
            "documents",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"cat".to_vec()))],
        )
        .unwrap();
    transaction.commit().unwrap();

    let row = db.rows_for("documents", None).unwrap().remove(0);
    assert_eq!(row.columns.get(&2), Some(&Value::Bytes(b"dog".to_vec())));
    assert_eq!(
        row.columns.get(&3),
        Some(&Value::Embedding(vec![3.0, 314.0]))
    );
}

#[test]
fn rls_rejection_happens_before_provider_receives_input() {
    let dir = tempdir().unwrap();
    let admin = Database::create_with_credentials(dir.path(), "admin", "admin-password").unwrap();
    admin.create_table("documents", schema()).unwrap();
    admin.create_user("alice", "alice-password").unwrap();
    admin.create_role("writer").unwrap();
    admin
        .grant_permission(
            "writer",
            Permission::Insert {
                table: "documents".into(),
            },
        )
        .unwrap();
    admin.grant_role("alice", "writer").unwrap();
    admin
        .set_security_catalog(SecurityCatalog {
            rls_tables: vec!["documents".into()],
            policies: vec![RowPolicy {
                name: "owner_only".into(),
                table: "documents".into(),
                command: PolicyCommand::Insert,
                subjects: vec!["public".into()],
                permissive: true,
                using: None,
                with_check: Some(SecurityExpr::ColumnEqCurrentUser { column: 2 }),
            }],
            masks: Vec::new(),
        })
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    admin
        .embedding_providers()
        .register_new(Arc::new(TextProvider::with_output(
            Arc::clone(&calls),
            ProviderOutput::Valid,
        )))
        .unwrap();
    let alice = admin.resolve_principal("alice").unwrap();

    let mut transaction = admin.begin_as(Some(alice));
    transaction
        .put(
            "documents",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"bob".to_vec()))],
        )
        .unwrap();
    assert!(transaction.commit().is_err());
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

#[test]
fn replication_applies_committed_vectors_without_provider() {
    let dir = tempdir().unwrap();
    let leader_path = dir.path().join("leader");
    let follower_path = dir.path().join("follower");
    let leader = Database::create(&leader_path).unwrap();
    leader
        .embedding_providers()
        .register_new(Arc::new(TextProvider::valid()))
        .unwrap();
    leader.create_table("documents", schema()).unwrap();

    let snapshot = leader.replication_snapshot().unwrap();
    snapshot.install(&follower_path).unwrap();

    let mut transaction = leader.begin();
    transaction
        .put(
            "documents",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"cat".to_vec()))],
        )
        .unwrap();
    transaction.commit().unwrap();
    let batch = leader.replication_batch_since(snapshot.epoch()).unwrap();

    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.embedding_providers().list_ids().is_empty());
    follower.append_replication_batch(&batch).unwrap();
    drop(follower);
    let follower = Database::open(&follower_path).unwrap();
    assert!(follower.embedding_providers().list_ids().is_empty());
    let row = follower.rows_for("documents", None).unwrap().remove(0);
    assert_eq!(
        row.columns.get(&3),
        Some(&Value::Embedding(vec![3.0, 312.0]))
    );
}
