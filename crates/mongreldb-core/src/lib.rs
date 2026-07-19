//! MongrelDB core — a log-structured columnar store with sub-ms writes, learned
//! indexes over a shared row-id space, page-level native encryption, an
//! MVCC-tagged content-addressed cache, and an AI-native access layer.
//!
//! The crate owns the WAL + memtable + Bε-tree write path, sorted-run container
//! formats, MVCC snapshots, page cache, encryption, compaction, and indexes.

#![allow(clippy::module_inception)]
#![recursion_limit = "2048"]

pub mod ai_generation;
pub mod auth;
pub mod auth_state;
pub mod backup;
pub mod be_tree;
pub mod cache;
pub mod catalog;
pub mod catalog_cmds;
pub mod certification;
pub mod cluster_import;
pub mod columnar;
pub mod commit_log;
pub mod compaction;
pub mod constraint;
pub mod core;
pub mod cursor;
pub mod database;
pub mod durable_file;
pub mod embedding;
pub mod encryption;
pub mod engine;
pub mod epoch;
pub mod error;
pub mod execution;
pub mod external_table;
pub mod gc;
pub mod global_idx;
pub mod handle;
pub mod index;
pub mod jobs;
pub mod locks;
pub mod manager;
pub mod manifest;
pub mod memory;
pub mod memtable;
pub mod migrate_mysql;
pub mod mutable_run;
pub mod node_governor;
pub mod ops_jobs;
pub mod page;
pub mod pitr;
pub mod pma;
pub mod procedure;
pub mod query;
pub mod replicated_apply;
pub mod replication;
pub mod reservoir;
pub mod resource;
pub mod retention;
pub(crate) mod row_id_set;
pub mod rowid;
pub mod scheduler;
pub mod schema;
pub mod security;
pub mod security_hardening;
pub mod sorted_run;
pub mod spill;
pub mod storage_mode;
pub mod trace;
pub mod trigger;
pub mod tsv;
pub mod txn;
pub mod wal;

pub(crate) const MAX_READ_GENERATION_LAYERS: usize = 8;

pub use crate::core::{DatabaseFileIdentity, LifecycleController, LifecycleState, OperationGuard};
pub use ai_generation::{
    evaluate_readiness, readiness_action, AiIndexGeneration, AiIndexGenerationRegistry, IndexId,
    IndexReadinessError, ReadinessAction,
};
pub use auth::{
    hash_password, verify_password, ColumnAccess, ColumnOperation, MysqlCachingSha2Verifier,
    Permission, Principal, RoleEntry, UserEntry,
};
pub use backup::{verify_backup, BackupFile, BackupManifest, BackupReport};
pub use be_tree::BeTree;
pub use cache::PageCache;
pub use catalog::{
    IncrementalAggregateKind, IncrementalAggregateOutput, IncrementalAggregateView,
    MaterializedViewEntry,
};
pub use catalog_cmds::{required_permission, CatalogCommand, CatalogCommandRecord, CatalogDelta};
pub use certification::{CertificationManifest, CertificationStatus, CertificationTest};
pub use cluster_import::{
    cluster_import_prepare, hash_rows_canonical, ImportPlan, ImportTablePlan,
};
pub use columnar::{decode_column, encode_column};
pub use cursor::{drain_cursor_to_columns, Cursor, MultiRunCursor, NativePageCursor};
pub use database::{
    lock_table_with_context, AuthorizedReadSnapshot, AuthorizedReadStamp, CdcBatch, ChangeEvent,
    CheckIssue, Database, DatabaseCore, DatabaseOpenMetrics, ExternalTriggerBaseWrite,
    ExternalTriggerBridge, ExternalTriggerWrite, ExternalTriggerWriteResult, OpenOptions,
    ReadAuthorization, TableGenerationStats, TableGuard, TableHandle, TablePinsReport,
    TableReadGeneration, DEFAULT_MEMORY_BUDGET_BYTES, DEFAULT_TEMP_DISK_BUDGET_BYTES,
};
pub use embedding::{
    EmbeddingError, EmbeddingFailurePolicy, EmbeddingFuture, EmbeddingLimits, EmbeddingModelMeta,
    EmbeddingNormalization, EmbeddingProvider, EmbeddingProviderRegistry, EmbeddingRequest,
    EmbeddingResponse, EmbeddingSource, FixedVectorProvider, GeneratedEmbeddingSpec,
    ProviderExecutionMode, ProviderHealth, ProviderStatus,
};
pub use encryption::{Cipher, PlaintextCipher};
pub use engine::{
    AggState, ApproxAgg, ApproxResult, CachedAgg, ColumnStat, IncrementalAggResult,
    IndexBuildPolicy, NativeAgg, NativeAggResult, ReadGeneration, Table, TableDeltas,
};
pub use epoch::{Epoch, EpochAuthority, EpochClock, MaintenanceReceipt, Snapshot};
pub use error::{MongrelError, Result};
pub use execution::{CancellationReason, ExecutionControl};
pub use external_table::{
    ExternalTableDefinition, ExternalTableEntry, ModuleArg, ModuleCapabilities,
};
pub use gc::{CheckReport, DoctorReport, GcReport, GcVersionsReport};
pub use handle::{DatabaseHandle, HandleAccess, HandleIdentity, OpenIdentity, SecretString};
pub use index::{
    AnnIndex, BitmapIndex, ColumnLearnedRange, FmIndex, HotIndex, IndexFamilyGeneration,
    IndexGeneration, LearnedIndex, SparseIndex,
};
pub use jobs::{
    CancellationToken, JobError, JobKind, JobProgress, JobRecord, JobRegistry, JobState, JobTarget,
    JOBS_FILENAME,
};
pub use locks::{LockError, LockKey, LockManager, LockMode, LockRequest};
pub use manager::DatabaseManager;
pub use manifest::TtlPolicy;
pub use memory::{
    EscalationLevel, EscalationThresholds, GovernorConfig, GovernorStats, MemoryClass, MemoryError,
    MemoryGovernor, Reclaimable, Reservation, SpillGrant,
};
pub use memtable::{Memtable, Row, Value};
pub use migrate_mysql::{
    dialect_matrix, map_mysql_type, plan_mysql_migration, run_migrate_pipeline,
    run_migrate_pipeline_controlled, CdcOp, DialectFeature, DialectSupport, MemoryMigrateIo,
    MigrateIo, MigrateRunReport, MigrateStage, MigrateTablePlan, MysqlMigratePlan,
    MysqlWireRequest, SourceRow, TypeMapping, DEFAULT_COPY_BATCH, DUAL_WRITE_WARNING,
};
pub use mutable_run::MutableRun;
pub use node_governor::{GovernorAction, NodeMemoryGovernor, NodePressureInputs};
pub use ops_jobs::{OpsJob, OpsJobError, OpsJobKind, OpsJobState, OpsJobStore};
pub use page::{CachedPage, Encoding, PageStat};
pub use pitr::{
    read_pitr_manifest, restore_pitr, PitrArchiveManifest, PitrArchiveReport, PitrChunkRef,
    PitrCommitPoint, PitrCredentials, PitrTarget,
};
pub use procedure::{
    ProcedureBody, ProcedureCallOutput, ProcedureCallResult, ProcedureCallRow, ProcedureCondition,
    ProcedureEntry, ProcedureMode, ProcedureParam, ProcedureStep, ProcedureValue, StoredProcedure,
};
pub use security_hardening::{
    node_cert_matches_id, redact_secrets, validate_jwt_claims, verify_jwt, IssuedServiceToken, Jwk,
    JwksCache, JwksDocument, JwksFetch, JwksProvider, JwtAlgorithm, JwtClaims, JwtError,
    JwtValidationConfig, KeyManagementError, KeyManagementHealth, KeyManagementProvider,
    KeyRotationJournal, KeyRotationPhase, KeyRotationRecord, KmsWrappedKey,
    ScramChannelBindingPolicy, ScramClientSession, ScramServerSession, ScramVerifier,
    SecurityHardeningError, ServiceToken, ServiceTokenRegistry, UnsupportedKeyManagementProvider,
    VerifiedJwt,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct BuildInfo {
    pub artifact_version: &'static str,
    pub engine_version: &'static str,
    pub mongreldb_git_sha: &'static str,
    pub target_triple: &'static str,
    pub build_profile: &'static str,
}

pub fn build_info() -> BuildInfo {
    BuildInfo {
        artifact_version: env!("CARGO_PKG_VERSION"),
        engine_version: env!("CARGO_PKG_VERSION"),
        mongreldb_git_sha: env!("MONGRELDB_GIT_SHA"),
        target_triple: env!("MONGRELDB_TARGET_TRIPLE"),
        build_profile: env!("MONGRELDB_BUILD_PROFILE"),
    }
}
pub use query::{Condition, Query};
pub use replicated_apply::{
    EngineSnapshot, EngineSnapshotFile, EngineSnapshotTable, ReplicatedTxnPayload,
    COMMAND_TYPE_CATALOG_COMMAND, COMMAND_TYPE_MAINTENANCE, ENGINE_SNAPSHOT_FORMAT_VERSION,
    REPLICATED_TXN_FORMAT_VERSION,
};
pub use replication::{
    is_replica, replica_epoch, replica_source_id, write_replica_epoch, ReplicationBatch,
    ReplicationSnapshot,
};
pub use reservoir::Reservoir;
pub use resource::{ResourceError, ResourceGroup, ResourceGroupRegistry, WorkloadClass};
pub use retention::{
    OwnedSnapshotGuard, PinGuard, PinInfo, PinRegistry, PinSource, PinsReport, SnapshotGuard,
    SnapshotRegistry,
};
pub use rowid::{RowId, RowIdAllocator};
pub use scheduler::{
    ClassConfig, ClassStats, HierarchicalScheduler, SchedulerError, SchedulerStats, TenantQuota,
    WorkItem,
};
pub use schema::{
    AlterColumn, ColumnDef, ColumnFlags, DefaultExpr, IndexDef, IndexKind, Schema, TypeId,
};
pub use security::{
    ColumnMask, MaskStrategy, PolicyCommand, RowPolicy, SecurityCatalog, SecurityExpr,
};
pub use sorted_run::{
    read_column_dir, read_header, write_run, write_run_with, ColumnPayload, RunHeader, RunReader,
    RunSpec, RunWriter,
};
pub use spill::{
    SpillConfig, SpillError, SpillHandle, SpillManager, SpillReader, SpillSession, SpillStats,
    SpillWriter,
};
pub use storage_mode::{StorageMode, StorageModeError, STORAGE_MODE_FILENAME};
pub use trace::{IndexRebuild, QueryTrace, ScanMode};
pub use trigger::{
    StoredTrigger, TriggerCell, TriggerCondition, TriggerConfig, TriggerDefinition, TriggerEntry,
    TriggerEvent, TriggerExpr, TriggerProgram, TriggerRaiseAction, TriggerStep, TriggerTarget,
    TriggerTiming, TriggerValue,
};
pub use txn::{IsolationLevel, OwnedRow, PutResult, UpsertAction, UpsertActionKind, UpsertResult};
pub use wal::{AddedRun, DdlOp, Op, Record, SharedWal, Wal, WalReader, SYSTEM_TXN_ID};

pub use encryption::{AesCipher, ColumnKeyDescriptor, EncryptionDescriptor, Kek};
