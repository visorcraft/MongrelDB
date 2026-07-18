//! AI index generations (spec section 13.3, Stage 4C).
//!
//! AI indexes remain local derived state. Each generation records definition
//! version, applied_through watermark, preprocessing/model versions, and
//! base/delta generation ids. Replicas build local graphs (no byte-identical
//! HNSW requirement). A replica may serve an indexed read only when
//! `applied_through >= requested read timestamp`; otherwise the caller waits,
//! routes elsewhere, uses exact fallback, or returns [`IndexNotReady`].

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::SchemaVersion;
use serde::{Deserialize, Serialize};

/// Opaque index identifier (stable within a database).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(transparent)]
pub struct IndexId(pub u64);

impl IndexId {
    /// Wrap a raw id.
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    /// Raw id.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for IndexId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One AI index generation (spec §13.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiIndexGeneration {
    /// Index identity.
    pub index_id: IndexId,
    /// Definition version (catalog/schema of the index).
    pub definition_version: u64,
    /// Highest commit timestamp whose rows are reflected in this generation.
    pub applied_through: HlcTimestamp,
    /// Source table schema version at build time.
    pub source_schema_version: SchemaVersion,
    /// Preprocessing pipeline version string.
    pub preprocessing_version: String,
    /// Optional embedding/model version.
    pub model_version: Option<String>,
    /// Base (full rebuild) generation id.
    pub base_generation: u64,
    /// Delta generation ids applied on top of the base.
    pub delta_generations: Vec<u64>,
}

/// Why an indexed AI read cannot be served from this replica.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IndexReadinessError {
    /// Index generation has not caught up to the requested read timestamp.
    #[error("index {index_id} not ready: applied_through {applied_through} < read_ts {read_ts}")]
    IndexNotReady {
        /// Index id.
        index_id: IndexId,
        /// Generation watermark.
        applied_through: HlcTimestamp,
        /// Requested read timestamp.
        read_ts: HlcTimestamp,
    },
    /// No generation is registered for the index.
    #[error("index {0} has no generation")]
    Missing(IndexId),
}

/// How the caller should react to an unreadiness result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadinessAction {
    /// Wait for local catch-up.
    Wait,
    /// Route the read to another replica.
    RouteElsewhere,
    /// Fall back to exact (non-ANN) retrieval.
    ExactFallback,
    /// Return IndexNotReady to the client.
    FailClosed,
}

/// Evaluate readiness of a generation against a requested read timestamp.
pub fn evaluate_readiness(
    generation: &AiIndexGeneration,
    read_ts: HlcTimestamp,
) -> Result<(), IndexReadinessError> {
    if generation.applied_through >= read_ts {
        Ok(())
    } else {
        Err(IndexReadinessError::IndexNotReady {
            index_id: generation.index_id,
            applied_through: generation.applied_through,
            read_ts,
        })
    }
}

/// Choose an action when readiness fails (policy helper for the gateway).
pub fn readiness_action(
    prefer_exact_fallback: bool,
    has_alternate_replica: bool,
    deadline_exhausted: bool,
) -> ReadinessAction {
    if deadline_exhausted {
        if prefer_exact_fallback {
            ReadinessAction::ExactFallback
        } else {
            ReadinessAction::FailClosed
        }
    } else if has_alternate_replica {
        ReadinessAction::RouteElsewhere
    } else if prefer_exact_fallback {
        ReadinessAction::ExactFallback
    } else {
        ReadinessAction::Wait
    }
}

/// Registry of AI index generations on one replica (local derived state).
#[derive(Debug, Default, Clone)]
pub struct AiIndexGenerationRegistry {
    by_index: std::collections::BTreeMap<IndexId, AiIndexGeneration>,
}

impl AiIndexGenerationRegistry {
    /// Empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish (or replace) a generation.
    pub fn publish(&mut self, generation: AiIndexGeneration) {
        self.by_index.insert(generation.index_id, generation);
    }

    /// Lookup.
    pub fn get(&self, index_id: IndexId) -> Option<&AiIndexGeneration> {
        self.by_index.get(&index_id)
    }

    /// Number of registered index generations.
    pub fn len(&self) -> usize {
        self.by_index.len()
    }

    /// Whether no generations are registered.
    pub fn is_empty(&self) -> bool {
        self.by_index.is_empty()
    }

    /// Serve check: ready for `read_ts` or typed error.
    pub fn require_ready(
        &self,
        index_id: IndexId,
        read_ts: HlcTimestamp,
    ) -> Result<&AiIndexGeneration, IndexReadinessError> {
        let gen = self
            .by_index
            .get(&index_id)
            .ok_or(IndexReadinessError::Missing(index_id))?;
        evaluate_readiness(gen, read_ts)?;
        Ok(gen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 1,
        }
    }

    fn gen(applied: u64) -> AiIndexGeneration {
        AiIndexGeneration {
            index_id: IndexId::new(7),
            definition_version: 1,
            applied_through: ts(applied),
            source_schema_version: SchemaVersion::new(1),
            preprocessing_version: "prep-1".into(),
            model_version: Some("embed-v2".into()),
            base_generation: 1,
            delta_generations: vec![2, 3],
        }
    }

    #[test]
    fn readiness_requires_applied_through_ge_read_ts() {
        let g = gen(100);
        assert!(evaluate_readiness(&g, ts(100)).is_ok());
        assert!(evaluate_readiness(&g, ts(50)).is_ok());
        let err = evaluate_readiness(&g, ts(101)).unwrap_err();
        assert!(matches!(err, IndexReadinessError::IndexNotReady { .. }));
    }

    #[test]
    fn registry_serve_and_missing() {
        let mut reg = AiIndexGenerationRegistry::new();
        reg.publish(gen(200));
        assert!(reg.require_ready(IndexId::new(7), ts(150)).is_ok());
        assert!(matches!(
            reg.require_ready(IndexId::new(9), ts(1)).unwrap_err(),
            IndexReadinessError::Missing(_)
        ));
    }

    #[test]
    fn readiness_action_policy() {
        assert_eq!(
            readiness_action(true, false, true),
            ReadinessAction::ExactFallback
        );
        assert_eq!(
            readiness_action(false, true, false),
            ReadinessAction::RouteElsewhere
        );
        assert_eq!(readiness_action(false, false, false), ReadinessAction::Wait);
        assert_eq!(
            readiness_action(false, false, true),
            ReadinessAction::FailClosed
        );
    }
}
