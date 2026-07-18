//! Bounded, cancellable embedding generation over materialized vectors.
//!
//! Schema stores portable provider/model identity. Node-local configuration
//! resolves that identity to model files or remote endpoints. Secrets and
//! filesystem paths never enter replicated schema.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_EMBEDDING_TEXTS: usize = 256;
pub const DEFAULT_MAX_EMBEDDING_TEXT_BYTES: usize = 64 * 1024;
pub const DEFAULT_MAX_EMBEDDING_INPUT_BYTES: usize = 1024 * 1024;
pub const DEFAULT_MAX_EMBEDDING_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingNormalization {
    #[default]
    None,
    L2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingFailurePolicy {
    /// Provider failure aborts the source write. Source and vector commit
    /// atomically or not at all.
    #[default]
    AbortWrite,
}

/// Portable generated-column contract stored in schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GeneratedEmbeddingSpec {
    pub provider_id: String,
    pub model_id: String,
    pub model_version: String,
    pub source_columns: Vec<u16>,
    /// Template placeholders use `{column_name}`. Empty means source values
    /// joined with one newline in `source_columns` order.
    pub input_template: String,
    pub dimension: u32,
    pub normalization: EmbeddingNormalization,
    pub failure_policy: EmbeddingFailurePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EmbeddingSource {
    #[default]
    SuppliedByApplication,
    /// A local provider resolved from node configuration. No path is stored.
    LocalModel {
        provider_id: String,
        model_id: String,
        model_version: String,
    },
    GeneratedColumn {
        spec: GeneratedEmbeddingSpec,
    },
}

impl EmbeddingSource {
    pub fn label(&self) -> &str {
        match self {
            Self::SuppliedByApplication => "supplied_by_application",
            Self::LocalModel { .. } => "local_model",
            Self::GeneratedColumn { .. } => "generated_column",
        }
    }

    pub fn provider_id(&self) -> Option<&str> {
        match self {
            Self::SuppliedByApplication => None,
            Self::LocalModel { provider_id, .. } => Some(provider_id),
            Self::GeneratedColumn { spec } => Some(&spec.provider_id),
        }
    }

    pub fn model_id(&self) -> Option<&str> {
        match self {
            Self::SuppliedByApplication => None,
            Self::LocalModel { model_id, .. } => Some(model_id),
            Self::GeneratedColumn { spec } => Some(&spec.model_id),
        }
    }

    pub fn model_version(&self) -> Option<&str> {
        match self {
            Self::SuppliedByApplication => None,
            Self::LocalModel { model_version, .. } => Some(model_version),
            Self::GeneratedColumn { spec } => Some(&spec.model_version),
        }
    }

    pub fn requires_provider(&self) -> bool {
        !matches!(self, Self::SuppliedByApplication)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EmbeddingError {
    #[error("embedding provider {0:?} is not registered")]
    ProviderNotFound(String),
    #[error("embedding provider {0:?} is already registered")]
    ProviderAlreadyRegistered(String),
    #[error(
        "embedding provider {provider:?} generation mismatch: expected {expected}, got {actual}"
    )]
    ProviderGenerationMismatch {
        provider: String,
        expected: u64,
        actual: u64,
    },
    #[error("embedding provider {provider:?} is referenced by {references} schema columns")]
    ProviderInUse { provider: String, references: usize },
    #[error("embedding provider {provider:?}: {message}")]
    ProviderFailed { provider: String, message: String },
    #[error("embedding source is SuppliedByApplication")]
    SuppliedByApplication,
    #[error("embedding provider identity mismatch: expected {expected:?}, got {got:?}")]
    ProviderIdentityMismatch { expected: String, got: String },
    #[error("embedding model identity mismatch: expected {expected:?}, got {got:?}")]
    ModelIdentityMismatch { expected: String, got: String },
    #[error("embedding model version mismatch: expected {expected:?}, got {got:?}")]
    ModelVersionMismatch { expected: String, got: String },
    #[error("embedding output count mismatch: expected {expected}, got {got}")]
    OutputCountMismatch { expected: usize, got: usize },
    #[error("embedding dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: u32, got: u32 },
    #[error("embedding request exceeds {resource}: requested {requested}, limit {limit}")]
    LimitExceeded {
        resource: &'static str,
        requested: usize,
        limit: usize,
    },
    #[error("embedding output is not finite")]
    NonFiniteOutput,
    #[error("embedding output is not L2 normalized")]
    NormalizationMismatch,
    #[error("embedding execution cancelled or timed out: {0}")]
    Execution(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderHealth {
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EmbeddingLimits {
    pub max_texts: usize,
    pub max_text_bytes: usize,
    pub max_total_input_bytes: usize,
    pub max_dimension: u32,
    pub max_output_bytes: usize,
}

impl Default for EmbeddingLimits {
    fn default() -> Self {
        Self {
            max_texts: DEFAULT_MAX_EMBEDDING_TEXTS,
            max_text_bytes: DEFAULT_MAX_EMBEDDING_TEXT_BYTES,
            max_total_input_bytes: DEFAULT_MAX_EMBEDDING_INPUT_BYTES,
            max_dimension: crate::schema::Schema::MAX_EMBEDDING_DIM,
            max_output_bytes: DEFAULT_MAX_EMBEDDING_OUTPUT_BYTES,
        }
    }
}

pub struct EmbeddingRequest<'a> {
    pub texts: &'a [&'a str],
    pub control: &'a crate::ExecutionControl,
    pub trace_id: &'a str,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingResponse {
    pub vectors: Vec<Vec<f32>>,
}

pub type EmbeddingFuture<'a> =
    Pin<Box<dyn Future<Output = Result<EmbeddingResponse, EmbeddingError>> + Send + 'a>>;

pub trait EmbeddingProvider: Send + Sync {
    fn provider_id(&self) -> &str;
    fn model_id(&self) -> &str;
    fn model_version(&self) -> &str;
    fn dimension(&self) -> u32;
    fn normalization(&self) -> EmbeddingNormalization;
    fn preprocessing_version(&self) -> &str;

    fn health(&self) -> ProviderHealth {
        ProviderHealth::Ready
    }

    /// Controlled synchronous inference used by transactional generated
    /// columns. Implementations must checkpoint during long work.
    fn embed(&self, request: EmbeddingRequest<'_>) -> Result<EmbeddingResponse, EmbeddingError>;

    /// Async transport hook for remote providers. The default is suitable only
    /// for short cooperative local inference; heavy local implementations must
    /// override this and use a bounded blocking executor.
    fn embed_async<'a>(&'a self, request: EmbeddingRequest<'a>) -> EmbeddingFuture<'a> {
        Box::pin(async move { self.embed(request) })
    }
}

#[derive(Clone)]
struct ProviderEntry {
    generation: u64,
    provider: Arc<dyn EmbeddingProvider>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderStatus {
    pub provider_id: String,
    pub model_id: String,
    pub model_version: String,
    pub generation: u64,
    pub health: ProviderHealth,
}

#[derive(Clone, Default)]
pub struct EmbeddingProviderRegistry {
    inner: Arc<RwLock<BTreeMap<String, ProviderEntry>>>,
}

impl std::fmt::Debug for EmbeddingProviderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddingProviderRegistry")
            .field("providers", &self.list_ids())
            .finish()
    }
}

impl EmbeddingProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_new(
        &self,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<u64, EmbeddingError> {
        let id = provider.provider_id().to_owned();
        let mut providers = self
            .inner
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if providers.contains_key(&id) {
            return Err(EmbeddingError::ProviderAlreadyRegistered(id));
        }
        providers.insert(
            id,
            ProviderEntry {
                generation: 1,
                provider,
            },
        );
        Ok(1)
    }

    pub fn replace(
        &self,
        expected_generation: u64,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Result<u64, EmbeddingError> {
        let id = provider.provider_id().to_owned();
        let mut providers = self
            .inner
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let entry = providers
            .get_mut(&id)
            .ok_or_else(|| EmbeddingError::ProviderNotFound(id.clone()))?;
        if entry.generation != expected_generation {
            return Err(EmbeddingError::ProviderGenerationMismatch {
                provider: id,
                expected: expected_generation,
                actual: entry.generation,
            });
        }
        entry.generation = entry.generation.saturating_add(1);
        entry.provider = provider;
        Ok(entry.generation)
    }

    pub(crate) fn unregister_unreferenced(
        &self,
        provider_id: &str,
        references: usize,
    ) -> Result<bool, EmbeddingError> {
        if references != 0 {
            return Err(EmbeddingError::ProviderInUse {
                provider: provider_id.to_owned(),
                references,
            });
        }
        Ok(self
            .inner
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(provider_id)
            .is_some())
    }

    pub fn get(&self, provider_id: &str) -> Option<Arc<dyn EmbeddingProvider>> {
        self.inner
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(provider_id)
            .map(|entry| Arc::clone(&entry.provider))
    }

    pub fn list_ids(&self) -> Vec<String> {
        self.inner
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    pub fn status(&self, provider_id: &str) -> Option<ProviderStatus> {
        let providers = self.inner.read().unwrap_or_else(|error| error.into_inner());
        let entry = providers.get(provider_id)?;
        Some(ProviderStatus {
            provider_id: provider_id.to_owned(),
            model_id: entry.provider.model_id().to_owned(),
            model_version: entry.provider.model_version().to_owned(),
            generation: entry.generation,
            health: entry.provider.health(),
        })
    }

    pub fn resolve(
        &self,
        source: &EmbeddingSource,
    ) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
        let provider_id = source
            .provider_id()
            .ok_or(EmbeddingError::SuppliedByApplication)?;
        self.get(provider_id)
            .ok_or_else(|| EmbeddingError::ProviderNotFound(provider_id.to_owned()))
    }

    pub fn embed(
        &self,
        source: &EmbeddingSource,
        texts: &[&str],
        expected_dim: u32,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        self.embed_controlled(
            source,
            texts,
            expected_dim,
            &crate::ExecutionControl::new(None),
            "embedding",
            EmbeddingLimits::default(),
        )
    }

    pub fn embed_controlled(
        &self,
        source: &EmbeddingSource,
        texts: &[&str],
        expected_dim: u32,
        control: &crate::ExecutionControl,
        trace_id: &str,
        limits: EmbeddingLimits,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        control
            .checkpoint()
            .map_err(|error| EmbeddingError::Execution(error.to_string()))?;
        validate_request(texts, expected_dim, limits)?;
        let provider = self.resolve(source)?;
        validate_identity(source, provider.as_ref())?;
        if provider.health() == ProviderHealth::Unavailable {
            return Err(EmbeddingError::ProviderFailed {
                provider: provider.provider_id().to_owned(),
                message: "provider unavailable".into(),
            });
        }
        if provider.dimension() != expected_dim {
            return Err(EmbeddingError::DimensionMismatch {
                expected: expected_dim,
                got: provider.dimension(),
            });
        }
        let response = provider.embed(EmbeddingRequest {
            texts,
            control,
            trace_id,
        })?;
        control
            .checkpoint()
            .map_err(|error| EmbeddingError::Execution(error.to_string()))?;
        validate_response(
            provider.as_ref(),
            texts.len(),
            expected_dim,
            limits,
            &response.vectors,
        )?;
        Ok(response.vectors)
    }
}

fn validate_identity(
    source: &EmbeddingSource,
    provider: &dyn EmbeddingProvider,
) -> Result<(), EmbeddingError> {
    if let EmbeddingSource::GeneratedColumn { spec } = source {
        if spec.normalization != provider.normalization() {
            return Err(EmbeddingError::NormalizationMismatch);
        }
        if spec.dimension != provider.dimension() {
            return Err(EmbeddingError::DimensionMismatch {
                expected: spec.dimension,
                got: provider.dimension(),
            });
        }
    }
    if source
        .provider_id()
        .is_some_and(|id| id != provider.provider_id())
    {
        return Err(EmbeddingError::ProviderIdentityMismatch {
            expected: source.provider_id().unwrap_or_default().to_owned(),
            got: provider.provider_id().to_owned(),
        });
    }
    if source
        .model_id()
        .is_some_and(|id| id != provider.model_id())
    {
        return Err(EmbeddingError::ModelIdentityMismatch {
            expected: source.model_id().unwrap_or_default().to_owned(),
            got: provider.model_id().to_owned(),
        });
    }
    if source
        .model_version()
        .is_some_and(|version| version != provider.model_version())
    {
        return Err(EmbeddingError::ModelVersionMismatch {
            expected: source.model_version().unwrap_or_default().to_owned(),
            got: provider.model_version().to_owned(),
        });
    }
    Ok(())
}

fn validate_request(
    texts: &[&str],
    dimension: u32,
    limits: EmbeddingLimits,
) -> Result<(), EmbeddingError> {
    if texts.len() > limits.max_texts {
        return Err(EmbeddingError::LimitExceeded {
            resource: "text count",
            requested: texts.len(),
            limit: limits.max_texts,
        });
    }
    if dimension > limits.max_dimension {
        return Err(EmbeddingError::LimitExceeded {
            resource: "dimension",
            requested: dimension as usize,
            limit: limits.max_dimension as usize,
        });
    }
    let mut total = 0usize;
    for text in texts {
        if text.len() > limits.max_text_bytes {
            return Err(EmbeddingError::LimitExceeded {
                resource: "text bytes",
                requested: text.len(),
                limit: limits.max_text_bytes,
            });
        }
        total = total.saturating_add(text.len());
    }
    if total > limits.max_total_input_bytes {
        return Err(EmbeddingError::LimitExceeded {
            resource: "total input bytes",
            requested: total,
            limit: limits.max_total_input_bytes,
        });
    }
    Ok(())
}

fn validate_response(
    provider: &dyn EmbeddingProvider,
    expected_count: usize,
    expected_dim: u32,
    limits: EmbeddingLimits,
    vectors: &[Vec<f32>],
) -> Result<(), EmbeddingError> {
    if vectors.len() != expected_count {
        return Err(EmbeddingError::OutputCountMismatch {
            expected: expected_count,
            got: vectors.len(),
        });
    }
    let output_bytes = vectors
        .len()
        .saturating_mul(expected_dim as usize)
        .saturating_mul(std::mem::size_of::<f32>());
    if output_bytes > limits.max_output_bytes {
        return Err(EmbeddingError::LimitExceeded {
            resource: "output bytes",
            requested: output_bytes,
            limit: limits.max_output_bytes,
        });
    }
    for vector in vectors {
        if vector.len() as u32 != expected_dim {
            return Err(EmbeddingError::DimensionMismatch {
                expected: expected_dim,
                got: vector.len() as u32,
            });
        }
        if vector.iter().any(|value| !value.is_finite()) {
            return Err(EmbeddingError::NonFiniteOutput);
        }
        if provider.normalization() == EmbeddingNormalization::L2 {
            let norm = vector
                .iter()
                .map(|value| (*value as f64) * (*value as f64))
                .sum::<f64>()
                .sqrt();
            if (norm - 1.0).abs() > 1e-4 {
                return Err(EmbeddingError::NormalizationMismatch);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct FixedVectorProvider {
    pub id: String,
    pub model_id: String,
    pub model_version: String,
    pub normalization: EmbeddingNormalization,
    pub vector: Vec<f32>,
}

impl EmbeddingProvider for FixedVectorProvider {
    fn provider_id(&self) -> &str {
        &self.id
    }
    fn model_id(&self) -> &str {
        &self.model_id
    }
    fn model_version(&self) -> &str {
        &self.model_version
    }
    fn dimension(&self) -> u32 {
        self.vector.len() as u32
    }
    fn normalization(&self) -> EmbeddingNormalization {
        self.normalization
    }
    fn preprocessing_version(&self) -> &str {
        "fixed-test-v1"
    }
    fn embed(&self, request: EmbeddingRequest<'_>) -> Result<EmbeddingResponse, EmbeddingError> {
        request
            .control
            .checkpoint()
            .map_err(|error| EmbeddingError::Execution(error.to_string()))?;
        Ok(EmbeddingResponse {
            vectors: request.texts.iter().map(|_| self.vector.clone()).collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EmbeddingModelMeta {
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
    pub model_version: Option<String>,
    pub preprocessing_version: Option<String>,
    pub normalization: EmbeddingNormalization,
    pub dimension: u32,
    pub source_kind: Option<String>,
}

impl EmbeddingModelMeta {
    pub fn from_provider(source: &EmbeddingSource, provider: &dyn EmbeddingProvider) -> Self {
        Self {
            provider_id: Some(provider.provider_id().to_owned()),
            model_id: Some(provider.model_id().to_owned()),
            model_version: Some(provider.model_version().to_owned()),
            preprocessing_version: Some(provider.preprocessing_version().to_owned()),
            normalization: provider.normalization(),
            dimension: provider.dimension(),
            source_kind: Some(source.label().to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> Arc<FixedVectorProvider> {
        Arc::new(FixedVectorProvider {
            id: "local-test".into(),
            model_id: "fixed".into(),
            model_version: "1".into(),
            normalization: EmbeddingNormalization::L2,
            vector: vec![0.0, 1.0],
        })
    }

    fn source() -> EmbeddingSource {
        EmbeddingSource::GeneratedColumn {
            spec: GeneratedEmbeddingSpec {
                provider_id: "local-test".into(),
                model_id: "fixed".into(),
                model_version: "1".into(),
                source_columns: vec![1],
                input_template: "{text}".into(),
                dimension: 2,
                normalization: EmbeddingNormalization::L2,
                failure_policy: EmbeddingFailurePolicy::AbortWrite,
            },
        }
    }

    #[test]
    fn registration_requires_expected_generation() {
        let registry = EmbeddingProviderRegistry::new();
        assert_eq!(registry.register_new(provider()).unwrap(), 1);
        assert!(matches!(
            registry.register_new(provider()),
            Err(EmbeddingError::ProviderAlreadyRegistered(_))
        ));
        assert!(matches!(
            registry.replace(9, provider()),
            Err(EmbeddingError::ProviderGenerationMismatch { .. })
        ));
        assert_eq!(registry.replace(1, provider()).unwrap(), 2);
        assert_eq!(registry.status("local-test").unwrap().generation, 2);
    }

    #[test]
    fn validates_count_dimension_finiteness_limits_and_cancellation() {
        let registry = EmbeddingProviderRegistry::new();
        registry.register_new(provider()).unwrap();
        assert_eq!(registry.embed(&source(), &["a", "b"], 2).unwrap().len(), 2);
        let control = crate::ExecutionControl::new(None);
        control.cancel(crate::CancellationReason::ClientRequest);
        assert!(matches!(
            registry.embed_controlled(
                &source(),
                &["a"],
                2,
                &control,
                "cancelled",
                EmbeddingLimits::default(),
            ),
            Err(EmbeddingError::Execution(_))
        ));
        assert!(matches!(
            registry.embed_controlled(
                &source(),
                &["too long"],
                2,
                &crate::ExecutionControl::new(None),
                "bounded",
                EmbeddingLimits {
                    max_text_bytes: 2,
                    ..EmbeddingLimits::default()
                },
            ),
            Err(EmbeddingError::LimitExceeded { .. })
        ));
    }

    struct WrongCount;
    impl EmbeddingProvider for WrongCount {
        fn provider_id(&self) -> &str {
            "wrong"
        }
        fn model_id(&self) -> &str {
            "wrong-model"
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
            "1"
        }
        fn embed(
            &self,
            _request: EmbeddingRequest<'_>,
        ) -> Result<EmbeddingResponse, EmbeddingError> {
            Ok(EmbeddingResponse {
                vectors: Vec::new(),
            })
        }
    }

    #[test]
    fn rejects_wrong_output_count() {
        let registry = EmbeddingProviderRegistry::new();
        registry.register_new(Arc::new(WrongCount)).unwrap();
        let source = EmbeddingSource::LocalModel {
            provider_id: "wrong".into(),
            model_id: "wrong-model".into(),
            model_version: "1".into(),
        };
        assert!(matches!(
            registry.embed(&source, &["a"], 2),
            Err(EmbeddingError::OutputCountMismatch {
                expected: 1,
                got: 0
            })
        ));
    }
}
