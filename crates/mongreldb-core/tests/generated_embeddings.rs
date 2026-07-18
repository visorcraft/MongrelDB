use std::sync::Arc;

use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::{
    Database, EmbeddingError, EmbeddingFailurePolicy, EmbeddingNormalization, EmbeddingProvider,
    EmbeddingRequest, EmbeddingResponse, EmbeddingSource, GeneratedEmbeddingSpec, Value,
};
use tempfile::tempdir;

struct TextProvider;

impl EmbeddingProvider for TextProvider {
    fn provider_id(&self) -> &str {
        "text-test"
    }

    fn model_id(&self) -> &str {
        "length-and-sum"
    }

    fn model_version(&self) -> &str {
        "1"
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
        Ok(EmbeddingResponse {
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
        })
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
        .register_new(Arc::new(TextProvider))
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
