//! P0.7 — embedding semantic identity and query-time retrieve_text.

use std::sync::Arc;

use mongreldb_core::schema::{
    AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind, IndexOptions, Schema,
    TypeId,
};
use mongreldb_core::{
    AnnIndex, Database, EmbeddingError, EmbeddingFailurePolicy, EmbeddingNormalization,
    EmbeddingProvider, EmbeddingProviderRef, EmbeddingRequest, EmbeddingResponse, EmbeddingSource,
    FixedVectorProvider, GeneratedEmbeddingMetadata, GeneratedEmbeddingSpec,
    GeneratedEmbeddingValue, ReEmbeddingCoordinator, ReEmbeddingState, TextSearchOptions, Value,
};
use sha2::{Digest, Sha256};
use tempfile::tempdir;

struct FingerprintProvider {
    id: String,
    model_id: String,
    model_version: String,
    provider_version: String,
    artifact: [u8; 32],
    dimension: u32,
    scale: f32,
}

impl FingerprintProvider {
    fn new(model_version: &str, artifact_byte: u8, scale: f32) -> Self {
        Self {
            id: "text-test".into(),
            model_id: "length-and-sum".into(),
            model_version: model_version.into(),
            provider_version: "1".into(),
            artifact: [artifact_byte; 32],
            dimension: 2,
            scale,
        }
    }
}

impl EmbeddingProvider for FingerprintProvider {
    fn provider_id(&self) -> &str {
        &self.id
    }
    fn model_id(&self) -> &str {
        &self.model_id
    }
    fn model_version(&self) -> &str {
        &self.model_version
    }
    fn provider_version(&self) -> &str {
        &self.provider_version
    }
    fn dimension(&self) -> u32 {
        self.dimension
    }
    fn normalization(&self) -> EmbeddingNormalization {
        EmbeddingNormalization::None
    }
    fn preprocessing_version(&self) -> &str {
        "raw-utf8-v1"
    }
    fn model_artifact_sha256(&self) -> [u8; 32] {
        self.artifact
    }
    fn tokenizer_sha256(&self) -> [u8; 32] {
        [0x11; 32]
    }
    fn preprocessing_sha256(&self) -> [u8; 32] {
        [0x22; 32]
    }
    fn embed(&self, request: EmbeddingRequest<'_>) -> Result<EmbeddingResponse, EmbeddingError> {
        Ok(EmbeddingResponse {
            vectors: request
                .texts
                .iter()
                .map(|text| {
                    vec![
                        self.scale * text.len() as f32,
                        self.scale * text.bytes().map(u32::from).sum::<u32>() as f32,
                    ]
                })
                .collect(),
        })
    }
}

fn documents_schema(with_ann: bool) -> Schema {
    let mut indexes = Vec::new();
    if with_ann {
        indexes.push(IndexDef {
            name: "embedding_ann".into(),
            column_id: 3,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    quantization: AnnQuantization::Dense,
                    ..AnnOptions::default()
                }),
                ..IndexOptions::default()
            },
        });
    }
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
                embedding_source: Some(EmbeddingSource::GeneratedColumnSpec {
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
        indexes,
        ..Schema::default()
    }
}

fn identity_a() -> EmbeddingProviderRef {
    FingerprintProvider::new("1", 0xAA, 1.0).semantic_identity()
}

fn identity_b() -> EmbeddingProviderRef {
    FingerprintProvider::new("1", 0xBB, 1.0).semantic_identity()
}

fn generated(vector: Vec<f32>, identity: EmbeddingProviderRef, generation: u64) -> Value {
    Value::GeneratedEmbedding(Box::new(GeneratedEmbeddingValue {
        vector,
        metadata: GeneratedEmbeddingMetadata {
            provider_id: identity.provider_id.clone(),
            model_id: identity.model_id.clone(),
            model_version: identity.model_version.clone(),
            preprocessing_version: "raw-utf8-v1".into(),
            source_fingerprint: Sha256::digest(b"synthetic").into(),
            status: mongreldb_core::EmbeddingGenerationStatus::Ready,
            last_error_category: None,
            attempt_count: 1,
            semantic_identity: identity,
            provider_registry_generation: generation,
        },
    }))
}

#[test]
fn same_dimension_different_fingerprint_rejected_on_ann() {
    let mut ann = AnnIndex::with_quantization(2, 8, 32, 16, AnnQuantization::Dense);
    let a = identity_a();
    let b = identity_b();
    assert_ne!(a.fingerprint_sha256(), b.fingerprint_sha256());
    assert_eq!(a.dimension, b.dimension);
    ann.bind_or_check_semantic_identity(&a).unwrap();
    assert_eq!(ann.semantic_identity(), Some(&a));
    let err = ann.bind_or_check_semantic_identity(&b).unwrap_err();
    assert!(matches!(
        err,
        EmbeddingError::AnnSemanticIdentityMismatch { .. }
    ));
}

#[test]
fn provider_replacement_cannot_alter_semantic_identity() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let gen = db
        .embedding_providers()
        .register_new(Arc::new(FingerprintProvider::new("1", 0xAA, 1.0)))
        .unwrap();
    assert_eq!(gen, 1);

    let err = db
        .embedding_providers()
        .replace(1, Arc::new(FingerprintProvider::new("1", 0xBB, 2.0)))
        .unwrap_err();
    assert!(matches!(
        err,
        EmbeddingError::SemanticIdentityImmutable { .. }
    ));

    let gen = db
        .embedding_providers()
        .replace(1, Arc::new(FingerprintProvider::new("1", 0xAA, 1.0)))
        .unwrap();
    assert_eq!(gen, 2);

    let gen = db
        .embedding_providers()
        .replace(2, Arc::new(FingerprintProvider::new("2", 0xBB, 2.0)))
        .unwrap();
    assert_eq!(gen, 3);
    let status = db.embedding_providers().status("text-test").unwrap();
    assert_eq!(status.semantic_identity.model_version, "2");
    assert_eq!(status.semantic_identity.model_artifact_sha256, [0xBB; 32]);
    assert_eq!(status.generation, 3);
}

#[test]
fn provider_generation_stored_with_vector_and_ann_binds_identity() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.embedding_providers()
        .register_new(Arc::new(FingerprintProvider::new("1", 0xAA, 1.0)))
        .unwrap();
    db.create_table("documents", documents_schema(true))
        .unwrap();

    let mut txn = db.begin();
    txn.put(
        "documents",
        vec![(1, Value::Int64(1)), (2, Value::Bytes(b"cat".to_vec()))],
    )
    .unwrap();
    txn.commit().unwrap();

    let row = db.rows_for("documents", None).unwrap().remove(0);
    let meta = row
        .columns
        .get(&3)
        .unwrap()
        .generated_embedding_metadata()
        .unwrap();
    assert_eq!(meta.provider_registry_generation, 1);
    assert_eq!(meta.semantic_identity, identity_a());
    assert_eq!(
        meta.source_fingerprint,
        <[u8; 32]>::from(Sha256::digest(b"cat"))
    );

    let handle = db.table("documents").unwrap();
    let mut table = handle.lock();
    table.ensure_indexes_complete().unwrap();
    let ann = table.ann_index(3).expect("ann index");
    assert_eq!(ann.semantic_identity(), Some(&identity_a()));
}

#[test]
fn query_time_embedding_selects_active_generation_and_returns_provenance() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.embedding_providers()
        .register_new(Arc::new(FingerprintProvider::new("1", 0xAA, 1.0)))
        .unwrap();
    db.create_table("documents", documents_schema(true))
        .unwrap();

    for (id, text) in [(1i64, "cat"), (2, "cats"), (3, "dog")] {
        let mut txn = db.begin();
        txn.put(
            "documents",
            vec![
                (1, Value::Int64(id)),
                (2, Value::Bytes(text.as_bytes().to_vec())),
            ],
        )
        .unwrap();
        txn.commit().unwrap();
    }

    let result = db
        .retrieve_text("documents", 3, "cat", TextSearchOptions::new(2))
        .unwrap();
    assert!(!result.hits.is_empty());
    assert_eq!(result.provenance.embedding_column, 3);
    assert_eq!(result.provenance.semantic_identity, identity_a());
    assert_eq!(result.provenance.provider_registry_generation, 1);
    assert_eq!(
        result.provenance.query_source_fingerprint,
        <[u8; 32]>::from(Sha256::digest(b"cat"))
    );
    let top = result.hits[0].row_id;
    let rows = db.rows_for("documents", None).unwrap();
    let top_row = rows.iter().find(|row| row.row_id == top).unwrap();
    assert_eq!(
        top_row.columns.get(&2),
        Some(&Value::Bytes(b"cat".to_vec()))
    );
}

#[test]
fn fixed_vector_provider_constructor_still_works() {
    let provider = FixedVectorProvider::new(
        "fixed",
        "m",
        "1",
        EmbeddingNormalization::L2,
        vec![0.0, 1.0],
    );
    let identity = provider.semantic_identity();
    assert_eq!(identity.provider_id, "fixed");
    assert_eq!(identity.dimension, 2);
    assert_ne!(identity.model_artifact_sha256, [0u8; 32]);
}

/// P0.7-X5 / X6 / X7: re-embedding publishes atomically; old model cannot
/// search the new index; interrupted backfill resumes from batch_cursor.
#[test]
fn reembedding_publish_atomic_old_model_rejected_and_resume() {
    let a = identity_a();
    let b = identity_b();
    assert_ne!(a.fingerprint_sha256(), b.fingerprint_sha256());

    let coord = ReEmbeddingCoordinator::new();
    let installed = coord.install_active(1, 3, a.clone());
    assert_eq!(installed.semantic_identity, a);

    let job = coord
        .start_reembedding(1, 3, b.clone(), 5)
        .expect("start re-embedding");
    assert_eq!(job.state, ReEmbeddingState::Pending);
    assert_eq!(
        job.source_identity.fingerprint_sha256(),
        a.fingerprint_sha256()
    );
    assert_eq!(
        job.target_identity.fingerprint_sha256(),
        b.fingerprint_sha256()
    );

    // Interrupted backfill: process two rows, then resume.
    let partial = coord.build_reembedding_batch(job.job_id, 2).unwrap();
    assert_eq!(partial.state, ReEmbeddingState::Running);
    assert_eq!(partial.batch_cursor, 2);
    assert_eq!(partial.rows_done, 2);

    // Active generation still serves the old identity during build.
    coord.require_active_identity(1, 3, &a).unwrap();
    let err = coord.require_active_identity(1, 3, &b).unwrap_err();
    assert!(matches!(
        err,
        EmbeddingError::AnnSemanticIdentityMismatch { .. }
    ));

    let ready = coord.build_reembedding_batch(job.job_id, 10).unwrap();
    assert_eq!(ready.state, ReEmbeddingState::Ready);
    assert_eq!(ready.batch_cursor, 5);

    let fence_before = coord.publish_fence();
    let published = coord.publish_reembedding(job.job_id).unwrap();
    assert!(published.publish_fence > fence_before);
    assert_eq!(coord.publish_fence(), published.publish_fence);

    // Atomic swap: new identity is active; old model cannot search new index.
    let active = coord.active_slot(1, 3).unwrap();
    assert_eq!(active.generation_id, job.hidden_generation_id);
    assert_eq!(
        active.semantic_identity.fingerprint_sha256(),
        b.fingerprint_sha256()
    );
    assert!(coord.hidden_slot(1, 3).is_none());

    let err = coord.require_active_identity(1, 3, &a).unwrap_err();
    assert!(matches!(
        err,
        EmbeddingError::AnnSemanticIdentityMismatch { .. }
    ));
    coord.require_active_identity(1, 3, &b).unwrap();
    assert!(coord.is_active_generation(1, 3, job.hidden_generation_id));
    assert!(!coord.is_active_generation(1, 3, job.source_generation_id));

    // Pin expiry retires the previous generation.
    let retired = coord.retire_old_after_pins(1, 3).unwrap().unwrap();
    assert_eq!(
        retired.semantic_identity.fingerprint_sha256(),
        a.fingerprint_sha256()
    );
}

#[test]
fn supplied_generated_embedding_with_mismatched_fingerprint_rejected() {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path()).unwrap();
    let schema = Schema {
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
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 2 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "embedding_ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    quantization: AnnQuantization::Dense,
                    ..AnnOptions::default()
                }),
                ..IndexOptions::default()
            },
        }],
        ..Schema::default()
    };
    db.create_table("vectors", schema).unwrap();

    let mut txn = db.begin();
    txn.put(
        "vectors",
        vec![
            (1, Value::Int64(1)),
            (2, generated(vec![1.0, 0.0], identity_a(), 1)),
        ],
    )
    .unwrap();
    txn.commit().unwrap();

    {
        let handle = db.table("vectors").unwrap();
        let mut table = handle.lock();
        table.ensure_indexes_complete().unwrap();
        assert_eq!(
            table.ann_index(2).unwrap().semantic_identity(),
            Some(&identity_a())
        );
    }

    let mut txn = db.begin();
    txn.put(
        "vectors",
        vec![
            (1, Value::Int64(2)),
            (2, generated(vec![0.0, 1.0], identity_b(), 1)),
        ],
    )
    .unwrap();
    let err = txn.commit().unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("semantic identity") || message.contains("ANN generation"),
        "unexpected error: {message}"
    );
}
