//! Pluggable embedding generation (optional layer over stored vectors).
//!
//! MongrelDB **never hard-codes an external embedding vendor**. Dense ANN
//! indexes operate only on already-materialized vectors plus model metadata.
//! Sparse retrieval needs no embedding model at all.
//!
//! # Sources
//!
//! - [`EmbeddingSource::SuppliedByApplication`] — the default: the client
//!   writes `Value::Embedding` directly. No generation runs inside the engine.
//! - [`EmbeddingSource::LocalModel`] — Kit or the server may load a local
//!   model from disk (bundled or operator-provided). Core only stores the
//!   path/id; a registered [`EmbeddingProvider`] performs inference.
//! - [`EmbeddingSource::GeneratedColumn`] — a named provider registered with
//!   the process (local or remote). The registry is process-local; storage
//!   remains vendor-independent.
//!
//! # Policy
//!
//! **Do not invent arbitrary dense vectors** (hashed pseudo-embeddings, random
//! noise, etc.) merely to claim Dense ANN is in use. A weak hashed vector can
//! perform worse than MongrelDB's native Sparse index while consuming more
//! storage and creating misleading "semantic search" expectations. Prefer
//! sparse retrieval when no real embedding model is available.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

/// Where embedding values for a column originate.
///
/// Catalog metadata only: the storage engine never calls out to a vendor
/// based on this enum. Resolution goes through [`EmbeddingProviderRegistry`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EmbeddingSource {
    /// Application supplies `Value::Embedding` on write. Default for every
    /// `TypeId::Embedding` column that omits an explicit source.
    #[default]
    SuppliedByApplication,
    /// Local on-disk model (Kit-bundled or operator-installed). Inference
    /// requires a registered provider that accepts this `model_id`.
    LocalModel {
        /// Filesystem path to model weights / tokenizer bundle.
        model_path: PathBuf,
        /// Stable model identity recorded with ANN generations.
        model_id: String,
    },
    /// Named provider registered on the server/process
    /// (`EmbeddingProviderRegistry::register`). May be local or remote; core
    /// does not interpret the provider string.
    GeneratedColumn {
        /// Registry key of the provider.
        provider: String,
    },
}

impl EmbeddingSource {
    /// Human-readable label for diagnostics and catalog dumps.
    pub fn label(&self) -> &str {
        match self {
            Self::SuppliedByApplication => "supplied_by_application",
            Self::LocalModel { .. } => "local_model",
            Self::GeneratedColumn { .. } => "generated_column",
        }
    }

    /// Model identity when known (for ANN generation metadata); `None` for
    /// application-supplied vectors that carry no model stamp.
    pub fn model_id(&self) -> Option<&str> {
        match self {
            Self::SuppliedByApplication => None,
            Self::LocalModel { model_id, .. } => Some(model_id.as_str()),
            Self::GeneratedColumn { provider } => Some(provider.as_str()),
        }
    }

    /// Whether the engine/provider layer is expected to materialize vectors
    /// (as opposed to the application writing them directly).
    pub fn requires_provider(&self) -> bool {
        !matches!(self, Self::SuppliedByApplication)
    }
}

/// Errors from the embedding provider layer (not storage).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EmbeddingError {
    /// No provider is registered under the requested id.
    #[error("embedding provider {0:?} is not registered")]
    ProviderNotFound(String),
    /// Provider exists but cannot produce vectors (missing model file, wrong
    /// dimension, remote unavailable, etc.).
    #[error("embedding provider {provider:?}: {message}")]
    ProviderFailed {
        /// Provider id.
        provider: String,
        /// Operator-facing detail (no secrets).
        message: String,
    },
    /// Application-supplied source was asked to generate vectors.
    #[error("embedding source is SuppliedByApplication; pass vectors from the client")]
    SuppliedByApplication,
    /// Caller asked for a generation path without configuring a source that
    /// can produce vectors.
    #[error("no embedding source configured for generation")]
    NoSource,
    /// Dimension mismatch between provider output and column definition.
    #[error("embedding dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// Column / index dimension.
        expected: u32,
        /// Provider output dimension.
        got: u32,
    },
}

/// One registered embedding backend (local model runner, remote HTTP adapter,
/// test double, …). Implementations live outside core storage; core only
/// holds the trait object in a process-local registry.
pub trait EmbeddingProvider: Send + Sync {
    /// Stable registry key (matches [`EmbeddingSource::GeneratedColumn`] /
    /// local model ids).
    fn provider_id(&self) -> &str;

    /// Model identity stamped onto ANN generations when this provider runs.
    fn model_id(&self) -> &str;

    /// Fixed output dimension; must match the column's `TypeId::Embedding { dim }`.
    fn dimension(&self) -> u32;

    /// Encode one or more texts into dense vectors of [`Self::dimension`].
    ///
    /// Implementations must **not** invent weak hashed stand-ins for real
    /// semantic models. Prefer returning [`EmbeddingError::ProviderFailed`]
    /// over producing misleading vectors.
    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}

/// Process-local registry of embedding providers.
///
/// Empty by default: dense search works when applications supply vectors;
/// sparse search works with no provider at all.
#[derive(Clone, Default)]
pub struct EmbeddingProviderRegistry {
    inner: Arc<RwLock<BTreeMap<String, Arc<dyn EmbeddingProvider>>>>,
}

impl std::fmt::Debug for EmbeddingProviderRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("EmbeddingProviderRegistry")
            .field("providers", &guard.keys().cloned().collect::<Vec<_>>())
            .finish()
    }
}

impl EmbeddingProviderRegistry {
    /// Empty registry (no vendors pre-registered).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register or replace a provider under its [`EmbeddingProvider::provider_id`].
    pub fn register(&self, provider: Arc<dyn EmbeddingProvider>) {
        let id = provider.provider_id().to_owned();
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.insert(id, provider);
    }

    /// Remove a provider by id. Returns whether it was present.
    pub fn unregister(&self, provider_id: &str) -> bool {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.remove(provider_id).is_some()
    }

    /// Lookup by registry key.
    pub fn get(&self, provider_id: &str) -> Option<Arc<dyn EmbeddingProvider>> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.get(provider_id).cloned()
    }

    /// Sorted list of registered provider ids.
    pub fn list_ids(&self) -> Vec<String> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.keys().cloned().collect()
    }

    /// Resolve a source to a provider, or refuse generation for
    /// application-supplied columns.
    pub fn resolve(
        &self,
        source: &EmbeddingSource,
    ) -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError> {
        match source {
            EmbeddingSource::SuppliedByApplication => Err(EmbeddingError::SuppliedByApplication),
            EmbeddingSource::LocalModel { model_id, .. } => self
                .get(model_id)
                .ok_or_else(|| EmbeddingError::ProviderNotFound(model_id.clone())),
            EmbeddingSource::GeneratedColumn { provider } => self
                .get(provider)
                .ok_or_else(|| EmbeddingError::ProviderNotFound(provider.clone())),
        }
    }

    /// Generate embeddings for `texts` under `source`, checking `expected_dim`.
    ///
    /// For [`EmbeddingSource::SuppliedByApplication`] this always errors —
    /// callers must write vectors themselves.
    pub fn embed(
        &self,
        source: &EmbeddingSource,
        texts: &[&str],
        expected_dim: u32,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let provider = self.resolve(source)?;
        if provider.dimension() != expected_dim {
            return Err(EmbeddingError::DimensionMismatch {
                expected: expected_dim,
                got: provider.dimension(),
            });
        }
        let vectors = provider.embed(texts)?;
        for v in &vectors {
            if v.len() as u32 != expected_dim {
                return Err(EmbeddingError::DimensionMismatch {
                    expected: expected_dim,
                    got: v.len() as u32,
                });
            }
            if v.iter().any(|x| !x.is_finite()) {
                return Err(EmbeddingError::ProviderFailed {
                    provider: provider.provider_id().to_owned(),
                    message: "provider returned non-finite embedding values".into(),
                });
            }
        }
        Ok(vectors)
    }
}

/// Test/demo provider that only echoes a fixed finite vector. **Not** a
/// semantic model — used solely to exercise the registry plumbing. Production
/// code should never present this as "semantic search."
#[derive(Debug, Clone)]
pub struct FixedVectorProvider {
    /// Registry key.
    pub id: String,
    /// Model stamp.
    pub model_id: String,
    /// Output vector (also defines dimension).
    pub vector: Vec<f32>,
}

impl EmbeddingProvider for FixedVectorProvider {
    fn provider_id(&self) -> &str {
        &self.id
    }

    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn dimension(&self) -> u32 {
        self.vector.len() as u32
    }

    fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        Ok(texts.iter().map(|_| self.vector.clone()).collect())
    }
}

/// Metadata recorded alongside an ANN index generation so readers know which
/// model produced the stored vectors (when known).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EmbeddingModelMeta {
    /// Optional model / provider identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    /// Optional human-readable source label (`supplied_by_application`, …).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_kind: Option<String>,
    /// Embedding dimension.
    pub dim: u32,
}

impl EmbeddingModelMeta {
    /// Build from a column source and dimension.
    pub fn from_source(source: &EmbeddingSource, dim: u32) -> Self {
        Self {
            model_id: source.model_id().map(str::to_owned),
            source_kind: Some(source.label().to_owned()),
            dim,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_source_is_application_supplied() {
        assert_eq!(
            EmbeddingSource::default(),
            EmbeddingSource::SuppliedByApplication
        );
        assert!(!EmbeddingSource::SuppliedByApplication.requires_provider());
    }

    #[test]
    fn registry_is_empty_by_default_no_vendor() {
        let reg = EmbeddingProviderRegistry::new();
        assert!(reg.list_ids().is_empty());
        assert!(matches!(
            reg.resolve(&EmbeddingSource::GeneratedColumn {
                provider: "openai".into()
            }),
            Err(EmbeddingError::ProviderNotFound(_))
        ));
    }

    #[test]
    fn supplied_by_application_refuses_generation() {
        let reg = EmbeddingProviderRegistry::new();
        let err = reg
            .embed(&EmbeddingSource::SuppliedByApplication, &["hello"], 4)
            .unwrap_err();
        assert!(matches!(err, EmbeddingError::SuppliedByApplication));
    }

    #[test]
    fn registered_provider_embeds_with_dim_check() {
        let reg = EmbeddingProviderRegistry::new();
        reg.register(Arc::new(FixedVectorProvider {
            id: "local-test".into(),
            model_id: "fixed-v1".into(),
            vector: vec![0.0, 1.0, 0.0, 0.0],
        }));
        let source = EmbeddingSource::GeneratedColumn {
            provider: "local-test".into(),
        };
        let out = reg.embed(&source, &["a", "b"], 4).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], vec![0.0, 1.0, 0.0, 0.0]);
        let dim_err = reg.embed(&source, &["a"], 8).unwrap_err();
        assert!(matches!(
            dim_err,
            EmbeddingError::DimensionMismatch {
                expected: 8,
                got: 4
            }
        ));
    }

    #[test]
    fn local_model_source_resolves_by_model_id() {
        let reg = EmbeddingProviderRegistry::new();
        reg.register(Arc::new(FixedVectorProvider {
            id: "kit-mini".into(),
            model_id: "kit-mini".into(),
            vector: vec![1.0, 0.0],
        }));
        let source = EmbeddingSource::LocalModel {
            model_path: PathBuf::from("/models/kit-mini"),
            model_id: "kit-mini".into(),
        };
        assert_eq!(source.model_id(), Some("kit-mini"));
        let out = reg.embed(&source, &["q"], 2).unwrap();
        assert_eq!(out[0].len(), 2);
    }

    #[test]
    fn model_meta_from_source() {
        let meta = EmbeddingModelMeta::from_source(&EmbeddingSource::SuppliedByApplication, 768);
        assert_eq!(meta.dim, 768);
        assert_eq!(meta.model_id, None);
        assert_eq!(meta.source_kind.as_deref(), Some("supplied_by_application"));
    }

    #[test]
    fn sparse_path_needs_no_provider() {
        // Documentation invariant: sparse retrieval is independent of this
        // registry. Presence of an empty registry must not block sparse.
        let reg = EmbeddingProviderRegistry::new();
        assert!(reg.list_ids().is_empty());
    }
}
