//! Online secondary-index create / drop / replace (MONGRELDB_TODO §4).
//!
//! Create and replace are driven as real [`JobKind::IndexBuild`] jobs through
//! [`run_build_publish`]. Publication is a short barrier under the commit lock:
//! the job constructs only the target hidden artifact from a pinned read
//! generation, refreshes from authoritative rows when the table generation
//! changes, then atomically publishes the staged schema + [`IndexGeneration`]
//! through the catalog/WAL path. Drop is a synchronous schema-only catalog
//! command with no table rewrite.
//!
//! Cluster replica roots reject this local user-DDL path (fail closed via the
//! shared `require` / `read_only` gates).

use super::*;
use crate::catalog_cmds::CatalogCommand;
use crate::jobs::{run_build_publish, BuildPublishJob, JobContext, JobError, JobKind, JobTarget};
use crate::retention::{PinGuard, PinSource};
use crate::schema::IndexDef;
use crate::wal::DdlOp;

/// Kind of online index-build job being driven.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum IndexBuildKind {
    Create {
        definition: IndexDef,
    },
    Replace {
        expected_old_name: String,
        new_definition: IndexDef,
    },
}

impl IndexBuildKind {
    fn definition(&self) -> &IndexDef {
        match self {
            Self::Create { definition } => definition,
            Self::Replace { new_definition, .. } => new_definition,
        }
    }
}

const INDEX_BUILD_SPEC_VERSION: u16 = 1;

/// Complete durable definition for reconstructing an index build after a
/// process restart. Phase watermarks stay in the bounded job checkpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct IndexBuildSpec {
    version: u16,
    table: String,
    expected_schema_sequence: u64,
    kind: IndexBuildKind,
}

impl IndexBuildSpec {
    fn encode(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self)
            .map_err(|error| MongrelError::Other(format!("serialize index-build spec: {error}")))
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let spec: Self = serde_json::from_slice(bytes)
            .map_err(|error| MongrelError::Other(format!("decode index-build spec: {error}")))?;
        if spec.version != INDEX_BUILD_SPEC_VERSION {
            return Err(MongrelError::Other(format!(
                "unsupported index-build spec version {}",
                spec.version
            )));
        }
        Ok(spec)
    }
}

/// In-memory driver state for one [`JobKind::IndexBuild`] drive.
struct IndexBuildJob<'a> {
    db: &'a Database,
    table: String,
    kind: IndexBuildKind,
    expected_schema_sequence: u64,
    /// Snapshot epoch pinned for the build (set in pin_snapshot).
    snapshot_epoch: Option<Epoch>,
    /// Live pin holding versions for the build (released on success/rollback).
    pin: Option<PinGuard>,
    /// Rows observed at the pinned snapshot (progress / validation only).
    snapshot_row_count: u64,
    /// Authoritative visible rows replayed through `built_through`.
    snapshot_rows: std::collections::BTreeMap<RowId, crate::memtable::Row>,
    /// Hidden target index built from `snapshot_rows`.
    artifact: Option<crate::engine::SecondaryIndexArtifact>,
    /// Governor charge covering the hidden rows, index, and build scratch.
    memory_reservation: Option<crate::memory::Reservation>,
    /// Exact table epoch covered by `artifact`.
    built_through: Option<Epoch>,
    /// Table-local content generation covered by `artifact`.
    built_data_generation: Option<u64>,
    /// Set once the durable catalog/generation publish completed.
    published: bool,
}

impl IndexBuildJob<'_> {
    fn catalog_command(&self) -> CatalogCommand {
        match &self.kind {
            IndexBuildKind::Create { definition } => CatalogCommand::AddIndex {
                table: self.table.clone(),
                index: definition.clone(),
            },
            IndexBuildKind::Replace {
                expected_old_name,
                new_definition,
            } => CatalogCommand::ReplaceIndex {
                table: self.table.clone(),
                expected_schema_sequence: self.expected_schema_sequence,
                expected_old_name: expected_old_name.clone(),
                new_definition: new_definition.clone(),
            },
        }
    }

    fn release_pin(&mut self) {
        self.pin = None;
    }

    fn reserve_hidden_memory(
        &mut self,
        schema: &Schema,
        rows: &[crate::memtable::Row],
    ) -> std::result::Result<(), JobError> {
        let row_bytes = rows.iter().fold(0u64, |total, row| {
            total.saturating_add(row.estimated_bytes().saturating_add(64))
        });
        let mut estimate = row_bytes.saturating_mul(2);
        if self.definition().kind == crate::schema::IndexKind::Ann {
            let dim = schema
                .columns
                .iter()
                .find(|column| column.id == self.definition().column_id)
                .and_then(|column| match column.ty {
                    crate::schema::TypeId::Embedding { dim } => Some(dim as u64),
                    _ => None,
                })
                .ok_or_else(|| {
                    JobError::Phase(format!(
                        "ANN index {} has no embedding column",
                        self.definition().name
                    ))
                })?;
            let options = self.definition().options.ann.clone().unwrap_or_default();
            let vector_bytes = match options.quantization {
                crate::schema::AnnQuantization::BinarySign => dim.div_ceil(8),
                crate::schema::AnnQuantization::Dense => dim.saturating_mul(4),
            };
            let levels = (crate::index::hnsw::MAX_HNSW_LEVEL as u64).saturating_add(1);
            let adjacency_slots = (options.m as u64)
                .saturating_mul(2)
                .saturating_add((options.m as u64).saturating_mul(levels.saturating_sub(1)));
            let per_node = vector_bytes
                .saturating_add(std::mem::size_of::<RowId>() as u64)
                .saturating_add(levels.saturating_mul(std::mem::size_of::<Vec<usize>>() as u64))
                .saturating_add(
                    adjacency_slots.saturating_mul(std::mem::size_of::<usize>() as u64),
                );
            // Persistent graph plus transient construction clones/heaps/sets.
            estimate = estimate
                .saturating_add(per_node.saturating_mul(rows.len() as u64).saturating_mul(2));
            estimate = estimate.saturating_add(
                (options.ef_construction as u64)
                    .saturating_mul(dim.saturating_add(32))
                    .saturating_mul(8),
            );
        }

        let requested = usize::try_from(estimate).unwrap_or(usize::MAX);
        let governor = self.db.memory_governor();
        let result = match self.memory_reservation.as_mut() {
            Some(reservation) => reservation.resize(estimate),
            None => governor
                .try_reserve(estimate, crate::memory::MemoryClass::Compaction)
                .map(|reservation| self.memory_reservation = Some(reservation)),
        };
        result.map_err(|error| {
            let limit = match error {
                crate::memory::MemoryError::Exhausted { available, .. } => {
                    usize::try_from(available).unwrap_or(usize::MAX)
                }
                crate::memory::MemoryError::LowPriorityRejected { .. } => 0,
                crate::memory::MemoryError::InvalidConfig(_) => {
                    usize::try_from(governor.class_budget(crate::memory::MemoryClass::Compaction))
                        .unwrap_or(usize::MAX)
                }
            };
            JobError::ResourceLimitExceeded {
                resource: "index build memory",
                requested,
                limit,
            }
        })
    }

    fn definition(&self) -> &IndexDef {
        self.kind.definition()
    }

    fn publication_already_applied(&self, schema: &Schema) -> bool {
        schema.schema_id == self.expected_schema_sequence.saturating_add(1)
            && schema
                .indexes
                .iter()
                .any(|index| index == self.definition())
            && match &self.kind {
                IndexBuildKind::Create { .. } => true,
                IndexBuildKind::Replace {
                    expected_old_name,
                    new_definition,
                } => {
                    expected_old_name == &new_definition.name
                        || !schema
                            .indexes
                            .iter()
                            .any(|index| index.name == *expected_old_name)
                }
            }
    }

    /// Refresh the hidden target from an authoritative pinned read generation.
    /// The row-map diff is the delta replay: missing row ids delete, present
    /// row ids insert or replace. Graph families are then rebuilt outside the
    /// final barrier because HNSW has no safe in-place delete.
    fn refresh_hidden(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        let handle = self.db.table(&self.table).map_err(job_phase_err)?;
        let (generation, snapshot) = handle
            .read_generation_with_context(None)
            .map_err(job_phase_err)?;
        if self.pin.is_none() {
            self.pin = Some(
                Arc::clone(generation.pin_registry())
                    .pin(PinSource::OnlineIndexBuild, snapshot.epoch),
            );
            self.snapshot_epoch.get_or_insert(snapshot.epoch);
        }

        let rows = generation.visible_rows(snapshot).map_err(job_phase_err)?;
        self.reserve_hidden_memory(generation.schema(), &rows)?;
        let next: std::collections::BTreeMap<_, _> =
            rows.into_iter().map(|row| (row.row_id, row)).collect();
        self.snapshot_rows
            .retain(|row_id, _| next.contains_key(row_id));
        for (row_id, row) in next {
            self.snapshot_rows.insert(row_id, row);
        }

        let rows: Vec<_> = self.snapshot_rows.values().cloned().collect();
        let total = rows.len();
        let definition = self.definition().clone();
        let mut last_reported = None;
        let artifact = generation
            .build_secondary_index_artifact(&definition, &rows, |done, total| {
                context.check_cancelled().map_err(MongrelError::from)?;
                if total > 0
                    && last_reported != Some(done)
                    && (done.is_multiple_of(256) || done == total)
                {
                    context
                        .report_progress(done as u64, total as u64)
                        .map_err(MongrelError::from)?;
                    last_reported = Some(done);
                }
                Ok(())
            })
            .map_err(job_phase_err)?;
        self.snapshot_row_count = total as u64;
        self.built_through = Some(snapshot.epoch);
        self.built_data_generation = Some(generation.data_generation());
        self.artifact = Some(artifact);
        Ok(())
    }
}

impl BuildPublishJob for IndexBuildJob<'_> {
    fn checkpoint_state(&self) -> Vec<u8> {
        // Bounded, small state only — never an unbounded delta stream.
        let epoch = self.snapshot_epoch.map(|e| e.0).unwrap_or(0);
        let built_through = self.built_through.map(|epoch| epoch.0).unwrap_or(0);
        let built_data_generation = self.built_data_generation.unwrap_or(0);
        let published = u8::from(self.published);
        let mut out = Vec::with_capacity(33);
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(&self.snapshot_row_count.to_le_bytes());
        out.extend_from_slice(&built_through.to_le_bytes());
        out.extend_from_slice(&built_data_generation.to_le_bytes());
        out.push(published);
        out
    }

    fn restore_checkpoint(&mut self, state: &[u8]) -> std::result::Result<(), JobError> {
        if state.len() < 17 {
            return Err(JobError::Phase(
                "index-build checkpoint is truncated".into(),
            ));
        }
        let mut epoch_bytes = [0u8; 8];
        epoch_bytes.copy_from_slice(&state[0..8]);
        let epoch = u64::from_le_bytes(epoch_bytes);
        if epoch != 0 {
            self.snapshot_epoch = Some(Epoch(epoch));
        }
        let mut count_bytes = [0u8; 8];
        count_bytes.copy_from_slice(&state[8..16]);
        self.snapshot_row_count = u64::from_le_bytes(count_bytes);
        if state.len() >= 33 {
            let mut built_bytes = [0u8; 8];
            built_bytes.copy_from_slice(&state[16..24]);
            let built = u64::from_le_bytes(built_bytes);
            if built != 0 {
                self.built_through = Some(Epoch(built));
            }
            let mut generation_bytes = [0u8; 8];
            generation_bytes.copy_from_slice(&state[24..32]);
            self.built_data_generation = Some(u64::from_le_bytes(generation_bytes));
            self.published = state[32] != 0;
        } else if state.len() >= 25 {
            let mut built_bytes = [0u8; 8];
            built_bytes.copy_from_slice(&state[16..24]);
            let built = u64::from_le_bytes(built_bytes);
            if built != 0 {
                self.built_through = Some(Epoch(built));
            }
            self.published = state[24] != 0;
        } else {
            self.published = state[16] != 0;
        }
        Ok(())
    }

    fn record_pending(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        // Job is already durable in the JOBS registry (submit). Validate the
        // definition still resolves against the live schema image.
        let catalog = self.db.catalog.read();
        let entry = catalog
            .live(&self.table)
            .ok_or_else(|| JobError::Phase(format!("table {:?} not found", self.table)))?;
        if entry.schema.schema_id != self.expected_schema_sequence {
            return Err(JobError::Phase(format!(
                "schema sequence changed before index build started on {:?}",
                self.table
            )));
        }
        let command = self.catalog_command();
        crate::catalog_cmds::apply(&catalog, &command).map_err(job_phase_err)?;
        Ok(())
    }

    fn pin_snapshot(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        if self.pin.is_some() {
            return Ok(());
        }
        let handle = self.db.table(&self.table).map_err(job_phase_err)?;
        let table = handle.lock();
        let epoch = table.current_epoch();
        let pin = Arc::clone(table.pin_registry()).pin(PinSource::OnlineIndexBuild, epoch);
        self.snapshot_epoch = Some(epoch);
        self.pin = Some(pin);
        Ok(())
    }

    fn build_hidden(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        self.refresh_hidden(context)
    }

    fn catch_up(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        self.refresh_hidden(context)
    }

    fn validate(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        let catalog = self.db.catalog.read();
        let entry = catalog
            .live(&self.table)
            .ok_or_else(|| JobError::Phase(format!("table {:?} not found", self.table)))?;
        if entry.schema.schema_id != self.expected_schema_sequence {
            return Err(JobError::Phase(format!(
                "schema sequence conflict during index build on {:?}: expected {}, found {}",
                self.table, self.expected_schema_sequence, entry.schema.schema_id
            )));
        }
        crate::catalog_cmds::apply(&catalog, &self.catalog_command()).map_err(job_phase_err)?;
        Ok(())
    }

    fn publish(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        if self.published {
            return Ok(());
        }
        match self.db.publish_index_build(self, context) {
            Ok(()) => {
                self.published = true;
                Ok(())
            }
            Err(error @ MongrelError::DurableCommit { .. }) => {
                let applied = self
                    .db
                    .catalog
                    .read()
                    .live(&self.table)
                    .is_some_and(|entry| self.publication_already_applied(&entry.schema));
                if applied {
                    self.published = true;
                    Ok(())
                } else {
                    Err(job_phase_err(error))
                }
            }
            Err(error) => Err(job_phase_err(error)),
        }
    }

    fn release_old(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        // Drop the build pin so version GC can advance. Old index generations
        // remain alive only while reader pins hold them (Arc).
        self.release_pin();
        self.artifact = None;
        self.memory_reservation = None;
        self.snapshot_rows.clear();
        Ok(())
    }

    fn rollback(&mut self) -> std::result::Result<(), JobError> {
        // Pre-publication cancel/failure: drop the pin and leave the old
        // schema + generation active. No hidden on-disk generation exists.
        self.release_pin();
        self.artifact = None;
        self.memory_reservation = None;
        self.snapshot_rows.clear();
        Ok(())
    }
}

fn job_phase_err(error: MongrelError) -> JobError {
    match error {
        MongrelError::Cancelled => JobError::Cancelled,
        MongrelError::Conflict(message) => JobError::Phase(message),
        other => JobError::Phase(other.to_string()),
    }
}

impl Database {
    /// Returns the immutable target definition recorded for one durable
    /// index-build job.
    ///
    /// Consumers use this to distinguish a resumable job for the requested
    /// representation from an obsolete job that must be cancelled before a
    /// replacement in the opposite direction is submitted.
    pub fn index_build_target_definition(&self, job_id: u64) -> Result<IndexDef> {
        let record = self
            .job_registry
            .get(job_id)
            .ok_or_else(|| MongrelError::NotFound(format!("job {job_id} not found")))?;
        if record.kind != JobKind::IndexBuild {
            return Err(MongrelError::InvalidArgument(format!(
                "job {job_id} is not an index build"
            )));
        }
        let definition = record.definition.as_deref().ok_or_else(|| {
            MongrelError::Other(format!(
                "index-build job {job_id} has no durable definition"
            ))
        })?;
        Ok(IndexBuildSpec::decode(definition)?
            .kind
            .definition()
            .clone())
    }

    /// Create a secondary index online, waiting for terminal success.
    ///
    /// SQL and local typed callers share this path. Cluster replica roots
    /// reject the mutation (fail closed).
    pub fn create_index(&self, table: &str, definition: IndexDef) -> Result<u64> {
        self.run_index_build(
            table,
            IndexBuildKind::Create {
                definition: definition.clone(),
            },
            Some(definition),
            None,
        )
    }

    /// Persist a create-index job without starting its worker.
    ///
    /// This is the queueing primitive for callers that own an executor. The
    /// returned id always names a durable `Pending` record containing the full
    /// versioned index definition.
    pub fn submit_create_index(&self, table: &str, definition: IndexDef) -> Result<u64> {
        let (job_id, _) = self.submit_index_build(
            table,
            IndexBuildKind::Create {
                definition: definition.clone(),
            },
            Some(definition),
            None,
        )?;
        Ok(job_id)
    }

    /// Persist and asynchronously drive a create-index job.
    pub fn start_create_index(self: &Arc<Self>, table: &str, definition: IndexDef) -> Result<u64> {
        let (job_id, spec) = self.submit_index_build(
            table,
            IndexBuildKind::Create {
                definition: definition.clone(),
            },
            Some(definition),
            None,
        )?;
        self.spawn_index_build(job_id, spec)?;
        Ok(job_id)
    }

    /// Atomically replace one secondary index with a new definition (e.g.
    /// BinarySign ↔ Dense ANN). Returns the durable job id after success.
    ///
    /// Publication is compare-and-swap on the table's `schema_id` captured at
    /// job start; concurrent DDL fails closed with [`MongrelError::Conflict`].
    pub fn replace_index(
        &self,
        table: &str,
        expected_old_name: &str,
        new_definition: IndexDef,
    ) -> Result<u64> {
        self.run_index_build(
            table,
            IndexBuildKind::Replace {
                expected_old_name: expected_old_name.to_string(),
                new_definition: new_definition.clone(),
            },
            Some(new_definition),
            Some(expected_old_name),
        )
    }

    /// Persist a replace-index job without starting its worker.
    pub fn submit_replace_index(
        &self,
        table: &str,
        expected_old_name: &str,
        new_definition: IndexDef,
    ) -> Result<u64> {
        let (job_id, _) = self.submit_index_build(
            table,
            IndexBuildKind::Replace {
                expected_old_name: expected_old_name.to_string(),
                new_definition: new_definition.clone(),
            },
            Some(new_definition),
            Some(expected_old_name),
        )?;
        Ok(job_id)
    }

    /// Persist and asynchronously drive a replace-index job.
    pub fn start_replace_index(
        self: &Arc<Self>,
        table: &str,
        expected_old_name: &str,
        new_definition: IndexDef,
    ) -> Result<u64> {
        let (job_id, spec) = self.submit_index_build(
            table,
            IndexBuildKind::Replace {
                expected_old_name: expected_old_name.to_string(),
                new_definition: new_definition.clone(),
            },
            Some(new_definition),
            Some(expected_old_name),
        )?;
        self.spawn_index_build(job_id, spec)?;
        Ok(job_id)
    }

    /// Requeue and asynchronously redrive a persisted index-build job after
    /// crash recovery or an operator pause.
    pub fn resume_index_build(self: &Arc<Self>, job_id: u64) -> Result<()> {
        let record = self
            .job_registry
            .get(job_id)
            .ok_or_else(|| MongrelError::NotFound(format!("job {job_id}")))?;
        if record.kind != JobKind::IndexBuild {
            return Err(MongrelError::InvalidArgument(format!(
                "job {job_id} is not an index build"
            )));
        }
        let definition = record.definition.as_deref().ok_or_else(|| {
            MongrelError::Other(format!(
                "index build job {job_id} has no reconstructible definition"
            ))
        })?;
        let spec = IndexBuildSpec::decode(definition)?;
        match record.state {
            crate::jobs::JobState::Paused => self
                .job_registry
                .resume(job_id)
                .map_err(MongrelError::from)?,
            crate::jobs::JobState::Pending => {}
            state => {
                return Err(MongrelError::Conflict(format!(
                    "index build job {job_id} cannot resume from {state:?}"
                )));
            }
        }
        self.spawn_index_build(job_id, spec)
    }

    /// Drop a secondary index by name. Schema + published generation advance
    /// atomically through the catalog commit path; table rows are not rewritten.
    pub fn drop_index(&self, table: &str, name: &str) -> Result<()> {
        use std::sync::atomic::Ordering;

        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        let _operation = self.admit_operation()?;
        let _schema_barrier = self.acquire_schema_barrier_exclusive()?;
        let _ddl = self.ddl_lock.lock();
        let _security_write = self.security_write()?;
        self.require(&crate::auth::Permission::Ddl)?;

        let table_id = {
            let catalog = self.catalog.read();
            catalog
                .live(table)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))?
                .table_id
        };
        let handle = self
            .tables
            .read()
            .get(&table_id)
            .cloned()
            .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not mounted")))?;

        let command = CatalogCommand::RemoveIndex {
            table: table.to_string(),
            name: name.to_string(),
        };
        // Pure pre-check before an epoch is consumed.
        let prepared_schema = match crate::catalog_cmds::apply(&self.catalog.read(), &command)? {
            crate::catalog_cmds::CatalogDelta::SchemaReplaced { schema, .. } => schema,
            crate::catalog_cmds::CatalogDelta::NoOp => return Ok(()),
            other => {
                return Err(MongrelError::Other(format!(
                    "unexpected catalog delta for drop_index: {other:?}"
                )));
            }
        };

        crate::catalog::inject_hook("index.publish.before")?;

        let durable_epoch = std::cell::Cell::new(None);
        let result: Result<()> = (|| {
            let mut table_guard = handle.lock();
            let commit_lock = Arc::clone(&self.commit_lock);
            let _commit = commit_lock.lock();
            let epoch = self.epoch.bump_assigned();
            let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
            let txn_id = self.alloc_txn_id()?;
            let mut next_catalog = self.catalog.read().clone();
            let catalog_entry_index = next_catalog
                .tables
                .iter()
                .position(|entry| entry.table_id == table_id)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))?;
            self.apply_catalog_command_to(&mut next_catalog, command)?;
            next_catalog.tables[catalog_entry_index].schema = prepared_schema.clone();
            next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);

            let commit_seq = {
                let mut wal = self.shared_wal.lock();
                let append: Result<u64> = (|| {
                    append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                    wal.append_commit(txn_id, epoch, &[])
                })();
                append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
            };
            let receipt = self.await_durable_commit(txn_id, commit_seq, epoch)?;
            durable_epoch.set(Some(epoch));

            table_guard.publish_index_drop(prepared_schema)?;
            let schema = table_guard.schema().clone();
            drop(table_guard);
            next_catalog.tables[catalog_entry_index].schema = schema;
            let catalog_result =
                catalog::write_atomic(&self.root, &next_catalog, self.meta_dek.as_ref());
            *self.catalog.write() = next_catalog;
            self.publish_committed(&receipt, epoch)?;
            epoch_guard.disarm();
            if let Err(error) = catalog_result {
                self.poisoned.store(true, Ordering::Relaxed);
                self.lifecycle.poison();
                return Err(MongrelError::DurableCommit {
                    epoch: epoch.0,
                    message: error.to_string(),
                });
            }
            crate::catalog::inject_hook("index.publish.after")?;
            Ok(())
        })();
        result.map_err(|error| match (durable_epoch.get(), error) {
            (_, error @ MongrelError::DurableCommit { .. }) => error,
            (Some(epoch), error) => MongrelError::DurableCommit {
                epoch: epoch.0,
                message: error.to_string(),
            },
            (None, error) => error,
        })
    }

    fn run_index_build(
        &self,
        table: &str,
        kind: IndexBuildKind,
        definition: Option<IndexDef>,
        expected_old_name: Option<&str>,
    ) -> Result<u64> {
        let (job_id, spec) = self.submit_index_build(table, kind, definition, expected_old_name)?;
        self.drive_index_build(job_id, spec)
    }

    fn submit_index_build(
        &self,
        table: &str,
        kind: IndexBuildKind,
        definition: Option<IndexDef>,
        expected_old_name: Option<&str>,
    ) -> Result<(u64, IndexBuildSpec)> {
        self.require(&crate::auth::Permission::Ddl)?;
        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        if table.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "index DDL requires a non-empty table name".into(),
            ));
        }

        // Validate against the live schema before allocating a job id.
        let (expected_schema_sequence, index_name) = {
            let catalog = self.catalog.read();
            let entry = catalog
                .live(table)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))?;
            if let Some(old) = expected_old_name {
                if !entry.schema.indexes.iter().any(|index| index.name == old) {
                    return Err(MongrelError::NotFound(format!(
                        "index {old} does not exist on {table}"
                    )));
                }
            }
            if let Some(def) = &definition {
                def.validate_options()?;
            }
            // Pure apply against a clone of the schema image (via catalog
            // command) catches name/column/AI conflicts fail closed.
            let command = match &kind {
                IndexBuildKind::Create { definition } => CatalogCommand::AddIndex {
                    table: table.to_string(),
                    index: definition.clone(),
                },
                IndexBuildKind::Replace {
                    expected_old_name,
                    new_definition,
                } => CatalogCommand::ReplaceIndex {
                    table: table.to_string(),
                    expected_schema_sequence: entry.schema.schema_id,
                    expected_old_name: expected_old_name.clone(),
                    new_definition: new_definition.clone(),
                },
            };
            crate::catalog_cmds::apply(&catalog, &command)?;
            let name = match &kind {
                IndexBuildKind::Create { definition } => definition.name.clone(),
                IndexBuildKind::Replace { new_definition, .. } => new_definition.name.clone(),
            };
            (entry.schema.schema_id, name)
        };

        // Ensure the table is mounted.
        let _ = self.table(table)?;

        let spec = IndexBuildSpec {
            version: INDEX_BUILD_SPEC_VERSION,
            table: table.to_string(),
            expected_schema_sequence,
            kind,
        };
        let job_id = self.job_registry.submit_with_definition(
            JobKind::IndexBuild,
            JobTarget {
                table: table.to_string(),
                index: Some(index_name),
            },
            Some(spec.encode()?),
        )?;
        Ok((job_id, spec))
    }

    fn spawn_index_build(self: &Arc<Self>, job_id: u64, spec: IndexBuildSpec) -> Result<()> {
        let db = Arc::clone(self);
        std::thread::Builder::new()
            .name(format!("mongreldb-index-{job_id}"))
            .spawn(move || {
                // Terminal state and error detail are persisted by the driver.
                let _ = db.drive_index_build(job_id, spec);
            })
            .map_err(|error| {
                MongrelError::Io(std::io::Error::new(
                    error.kind(),
                    format!("spawn index build job {job_id}: {error}"),
                ))
            })?;
        Ok(())
    }

    fn drive_index_build(&self, job_id: u64, spec: IndexBuildSpec) -> Result<u64> {
        let mut job = IndexBuildJob {
            db: self,
            table: spec.table,
            kind: spec.kind,
            expected_schema_sequence: spec.expected_schema_sequence,
            snapshot_epoch: None,
            pin: None,
            snapshot_row_count: 0,
            snapshot_rows: std::collections::BTreeMap::new(),
            artifact: None,
            memory_reservation: None,
            built_through: None,
            built_data_generation: None,
            published: false,
        };

        match run_build_publish(self.job_registry.as_ref(), job_id, &mut job) {
            Ok(()) => {
                let record = self.job_registry.get(job_id);
                match record.map(|r| r.state) {
                    Some(crate::jobs::JobState::Succeeded) => Ok(job_id),
                    Some(crate::jobs::JobState::Paused) => Err(MongrelError::Other(format!(
                        "index build job {job_id} parked mid-run; resume and redrive"
                    ))),
                    Some(state) => Err(MongrelError::Other(format!(
                        "index build job {job_id} ended in unexpected state {state:?}"
                    ))),
                    None => Err(MongrelError::NotFound(format!("job {job_id}"))),
                }
            }
            Err(JobError::Cancelled) => {
                // Cancellation after durable publication is reported as success
                // of the committed result (phase publish sets published=true
                // before release; cancel then cannot roll it back).
                if job.published
                    || self
                        .job_registry
                        .get(job_id)
                        .is_some_and(|r| r.state == crate::jobs::JobState::Succeeded)
                {
                    Ok(job_id)
                } else {
                    Err(MongrelError::Cancelled)
                }
            }
            Err(error) => {
                if job.published {
                    // Committed: surface success of the durable outcome.
                    Ok(job_id)
                } else {
                    Err(error.into())
                }
            }
        }
    }

    /// Short publication barrier for an index-build job. If the table changed
    /// after catch-up, release every barrier, refresh the hidden artifact, and
    /// retry. The barrier itself performs only CAS, WAL/catalog publication,
    /// and an O(number of indexed columns) generation swap.
    fn publish_index_build(&self, job: &mut IndexBuildJob<'_>, context: &JobContext) -> Result<()> {
        use std::sync::atomic::Ordering;

        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        // Re-check DDL auth at publication (revocation must stay effective).
        self.require(&crate::auth::Permission::Ddl)?;

        let table_name = job.table.clone();
        let table_id = {
            let catalog = self.catalog.read();
            let entry = catalog
                .live(&table_name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?;
            if entry.schema.schema_id != job.expected_schema_sequence {
                if job.publication_already_applied(&entry.schema) {
                    job.published = true;
                    return Ok(());
                }
                return Err(MongrelError::Conflict(format!(
                    "index publish on {table_name}: expected schema sequence {}, found {}",
                    job.expected_schema_sequence, entry.schema.schema_id
                )));
            }
            entry.table_id
        };
        let handle =
            self.tables.read().get(&table_id).cloned().ok_or_else(|| {
                MongrelError::NotFound(format!("table {table_name:?} not mounted"))
            })?;

        loop {
            context.check_cancelled().map_err(MongrelError::from)?;
            if job.artifact.is_none() || job.built_data_generation.is_none() {
                job.refresh_hidden(context).map_err(MongrelError::from)?;
            }
            let built_data_generation = job.built_data_generation.ok_or_else(|| {
                MongrelError::Other("index build has no content watermark".into())
            })?;
            let command = job.catalog_command();
            let prepared_schema = match crate::catalog_cmds::apply(&self.catalog.read(), &command)?
            {
                crate::catalog_cmds::CatalogDelta::SchemaReplaced { schema, .. } => schema,
                other => {
                    return Err(MongrelError::Other(format!(
                        "unexpected catalog delta for index publish: {other:?}"
                    )));
                }
            };
            let durable_epoch = std::cell::Cell::new(None);

            // Jobs run without the schema barrier during scan/build. Take it
            // only for the exact-CAS and publication window.
            let _schema_barrier = self.acquire_schema_barrier_exclusive()?;
            let _ddl = self.ddl_lock.lock();
            let _security_write = self.security_write()?;
            self.require(&crate::auth::Permission::Ddl)?;

            // Re-check CAS under the barrier.
            {
                let catalog = self.catalog.read();
                let entry = catalog.live(&table_name).ok_or_else(|| {
                    MongrelError::NotFound(format!("table {table_name:?} not found"))
                })?;
                if entry.schema.schema_id != job.expected_schema_sequence {
                    if job.publication_already_applied(&entry.schema) {
                        job.published = true;
                        return Ok(());
                    }
                    return Err(MongrelError::Conflict(format!(
                        "index publish on {table_name}: expected schema sequence {}, found {}",
                        job.expected_schema_sequence, entry.schema.schema_id
                    )));
                }
            }

            // Global lock order is commit lock -> table lock.
            let commit_lock = Arc::clone(&self.commit_lock);
            let _commit = commit_lock.lock();
            let mut table_guard = handle.lock();
            if table_guard.data_generation() != built_data_generation {
                drop(table_guard);
                drop(_commit);
                drop(_security_write);
                drop(_ddl);
                drop(_schema_barrier);
                job.refresh_hidden(context).map_err(MongrelError::from)?;
                continue;
            }

            let result: Result<()> = (|| {
                // Fail closed immediately before the durable catalog/generation
                // boundary. Tests arm barriers here, never sleeps.
                crate::catalog::inject_hook("index.publish.before")?;

                let epoch = self.epoch.bump_assigned();
                let mut epoch_guard = EpochGuard::new(self.epoch.as_ref(), epoch);
                let txn_id = self.alloc_txn_id()?;
                let mut next_catalog = self.catalog.read().clone();
                let catalog_entry_index = next_catalog
                    .tables
                    .iter()
                    .position(|entry| entry.table_id == table_id)
                    .ok_or_else(|| {
                        MongrelError::NotFound(format!("table {table_name:?} not found"))
                    })?;
                self.apply_catalog_command_to(&mut next_catalog, command)?;
                next_catalog.tables[catalog_entry_index].schema = prepared_schema.clone();
                next_catalog.db_epoch = next_catalog.db_epoch.max(epoch.0);

                let commit_seq = {
                    let mut wal = self.shared_wal.lock();
                    let append: Result<u64> = (|| {
                        // Catalog snapshot is the durable record of the index DDL.
                        let _ = DdlOp::encode_schema(&prepared_schema)?;
                        append_catalog_snapshot(&mut wal, txn_id, &next_catalog)?;
                        wal.append_commit(txn_id, epoch, &[])
                    })();
                    append.map_err(|error| self.commit_outcome_unknown(epoch, error))?
                };
                let receipt = self.await_durable_commit(txn_id, commit_seq, epoch)?;
                durable_epoch.set(Some(epoch));

                let artifact = job.artifact.take().ok_or_else(|| {
                    MongrelError::Other("index build lost its hidden artifact".into())
                })?;
                table_guard.publish_index_schema_change(prepared_schema, artifact)?;
                let schema = table_guard.schema().clone();
                drop(table_guard);

                next_catalog.tables[catalog_entry_index].schema = schema;
                let catalog_result =
                    catalog::write_atomic(&self.root, &next_catalog, self.meta_dek.as_ref());
                *self.catalog.write() = next_catalog;
                self.publish_committed(&receipt, epoch)?;
                epoch_guard.disarm();
                if let Err(error) = catalog_result {
                    self.poisoned.store(true, Ordering::Relaxed);
                    self.lifecycle.poison();
                    return Err(MongrelError::DurableCommit {
                        epoch: epoch.0,
                        message: error.to_string(),
                    });
                }
                crate::catalog::inject_hook("index.publish.after")?;
                Ok(())
            })();
            return result.map_err(|error| match (durable_epoch.get(), error) {
                (_, error @ MongrelError::DurableCommit { .. }) => error,
                (Some(epoch), error) => MongrelError::DurableCommit {
                    epoch: epoch.0,
                    message: error.to_string(),
                },
                (None, error) => error,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{Retriever, RetrieverScore};
    use crate::schema::{AnnOptions, AnnQuantization, IndexKind, IndexOptions, TypeId};
    use std::sync::Mutex;

    /// Serializes unit tests that touch the process-global fault registry or
    /// share online-index publication hooks with concurrent cases.
    static INDEX_DDL_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn lock_tests() -> std::sync::MutexGuard<'static, ()> {
        INDEX_DDL_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn embedding_schema(quantization: AnnQuantization) -> Schema {
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
                    name: "embedding".into(),
                    ty: TypeId::Embedding { dim: 4 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "idx_embed_ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: IndexOptions {
                    ann: Some(AnnOptions {
                        m: 8,
                        ef_construction: 32,
                        ef_search: 16,
                        quantization,
                    }),
                    ..IndexOptions::default()
                },
            }],
            ..Schema::default()
        }
    }

    fn plain_embedding_schema() -> Schema {
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
                    name: "embedding".into(),
                    ty: TypeId::Embedding { dim: 4 },
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            ..Schema::default()
        }
    }

    fn put_row(db: &Database, table: &str, id: i64, embedding: Vec<f32>) {
        let mut txn = db.begin();
        txn.put(
            table,
            vec![(1, Value::Int64(id)), (2, Value::Embedding(embedding))],
        )
        .unwrap();
        txn.commit().unwrap();
    }

    fn row_id_for_pk(db: &Database, table: &str, id: i64) -> RowId {
        let handle = db.table(table).unwrap();
        let table = handle.lock();
        table
            .visible_rows(Snapshot::at(table.current_epoch()))
            .unwrap()
            .into_iter()
            .find(|row| row.columns.get(&1) == Some(&Value::Int64(id)))
            .map(|row| row.row_id)
            .unwrap()
    }

    fn dense_definition() -> IndexDef {
        IndexDef {
            name: "idx_embed_ann".into(),
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
        }
    }

    #[test]
    fn create_index_indexes_snapshot_rows() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", plain_embedding_schema()).unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
        put_row(&db, "docs", 2, vec![0.0, 1.0, 0.0, 0.0]);

        let definition = IndexDef {
            name: "idx_embed_ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    m: 8,
                    ef_construction: 32,
                    ef_search: 16,
                    quantization: AnnQuantization::Dense,
                }),
                ..IndexOptions::default()
            },
        };
        let job_id = db.create_index("docs", definition).unwrap();
        let record = db.job_registry().get(job_id).unwrap();
        assert_eq!(record.state, crate::jobs::JobState::Succeeded);

        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        let hits = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: vec![1.0, 0.0, 0.0, 0.0],
                k: 2,
            })
            .unwrap();
        assert_eq!(hits.len(), 2);
        assert!(matches!(
            hits[0].score,
            RetrieverScore::AnnCosineDistance(d) if d.abs() < 1e-5
        ));
        // Schema sequence advanced once for the create (initial schema_id is
        // the allocated table_id, then AddIndex bumps by one).
        assert_eq!(table.schema().schema_id, table.table_id().saturating_add(1));
    }

    #[test]
    fn replace_binary_sign_with_dense_search_works() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", embedding_schema(AnnQuantization::BinarySign))
            .unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
        put_row(&db, "docs", 2, vec![0.0, 1.0, 0.0, 0.0]);

        let new_def = IndexDef {
            name: "idx_embed_ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    m: 8,
                    ef_construction: 32,
                    ef_search: 16,
                    quantization: AnnQuantization::Dense,
                }),
                ..IndexOptions::default()
            },
        };
        db.replace_index("docs", "idx_embed_ann", new_def).unwrap();

        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        let hits = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: vec![1.0, 0.0, 0.0, 0.0],
                k: 1,
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(matches!(
            hits[0].score,
            RetrieverScore::AnnCosineDistance(_)
        ));
        assert!(!hits
            .iter()
            .any(|hit| matches!(hit.score, RetrieverScore::AnnHammingDistance(_))));
    }

    #[test]
    fn drop_index_removes_schema_entry_without_rewrite() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", embedding_schema(AnnQuantization::Dense))
            .unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
        db.drop_index("docs", "idx_embed_ann").unwrap();
        let handle = db.table("docs").unwrap();
        let table = handle.lock();
        assert!(table.schema().indexes.is_empty());
        // Rows remain.
        let rows = table
            .visible_rows(Snapshot::at(table.current_epoch()))
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn concurrent_writes_progress_during_create() {
        use std::sync::Barrier;
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_table("docs", plain_embedding_schema()).unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);

        let start = Arc::new(Barrier::new(2));
        let writer = {
            let db = Arc::clone(&db);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for id in 100..120 {
                    put_row(&db, "docs", id, vec![0.0, 1.0, 0.0, 0.0]);
                }
            })
        };
        start.wait();
        let definition = IndexDef {
            name: "idx_embed_ann".into(),
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
        };
        db.create_index("docs", definition).unwrap();
        writer.join().unwrap();
        // Writers completed; index is published.
        let handle = db.table("docs").unwrap();
        let table = handle.lock();
        assert!(table
            .schema()
            .indexes
            .iter()
            .any(|index| index.name == "idx_embed_ann"));
    }

    #[test]
    fn asynchronous_build_is_observable_and_replays_all_row_deltas() {
        use std::sync::Barrier;
        use std::time::Duration;

        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_table("docs", plain_embedding_schema()).unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
        put_row(&db, "docs", 3, vec![0.0, 0.0, 1.0, 0.0]);
        let deleted_row = row_id_for_pk(&db, "docs", 3);
        let baseline = db
            .memory_governor()
            .usage(crate::memory::MemoryClass::Compaction);

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let callback_entered = Arc::clone(&entered);
        let callback_release = Arc::clone(&release);
        let _guard = mongreldb_fault::ScopedGuard::new(
            "job.build_hidden.after",
            mongreldb_fault::Action::Callback(Arc::new(move |_| {
                callback_entered.wait();
                callback_release.wait();
            })),
        );

        let job_id = db.start_create_index("docs", dense_definition()).unwrap();
        entered.wait();

        let running = db.job_registry().get(job_id).unwrap();
        assert_eq!(running.state, crate::jobs::JobState::Running);
        let persisted = IndexBuildSpec::decode(running.definition.as_deref().unwrap()).unwrap();
        assert_eq!(persisted.table, "docs");
        assert_eq!(persisted.kind.definition(), &dense_definition());
        assert_eq!(
            db.index_build_target_definition(job_id).unwrap(),
            dense_definition()
        );
        assert!(
            db.memory_governor()
                .usage(crate::memory::MemoryClass::Compaction)
                > baseline,
            "hidden generation must hold a governor reservation"
        );

        // These commits land after the initial hidden graph was built and
        // before catch-up: one update, one insert, and one delete.
        put_row(&db, "docs", 1, vec![0.0, 1.0, 0.0, 0.0]);
        put_row(&db, "docs", 2, vec![0.0, 1.0, 0.0, 0.0]);
        db.transaction(|txn| txn.delete("docs", deleted_row))
            .unwrap();

        release.wait();
        let terminal = db
            .job_registry()
            .wait_terminal(job_id, Duration::from_secs(30))
            .unwrap();
        assert_eq!(terminal.state, crate::jobs::JobState::Succeeded);
        assert_eq!(
            db.memory_governor()
                .usage(crate::memory::MemoryClass::Compaction),
            baseline,
            "successful build must release its reservation"
        );

        let expected: std::collections::HashSet<_> = [1, 2]
            .into_iter()
            .map(|id| row_id_for_pk(&db, "docs", id))
            .collect();
        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        let actual: std::collections::HashSet<_> = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: vec![0.0, 1.0, 0.0, 0.0],
                k: 10,
            })
            .unwrap()
            .into_iter()
            .map(|hit| hit.row_id)
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cancelling_real_hidden_build_rolls_back_pin_memory_and_schema() {
        use std::sync::Barrier;
        use std::time::Duration;

        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_table("docs", plain_embedding_schema()).unwrap();
        for id in 0..32 {
            put_row(&db, "docs", id, vec![1.0, id as f32, 0.0, 0.0]);
        }
        let baseline = db
            .memory_governor()
            .usage(crate::memory::MemoryClass::Compaction);

        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let callback_entered = Arc::clone(&entered);
        let callback_release = Arc::clone(&release);
        let _guard = mongreldb_fault::ScopedGuard::new(
            "job.build_hidden.after",
            mongreldb_fault::Action::Callback(Arc::new(move |_| {
                callback_entered.wait();
                callback_release.wait();
            })),
        );

        let job_id = db.start_create_index("docs", dense_definition()).unwrap();
        entered.wait();
        db.job_registry().cancel(job_id).unwrap();
        release.wait();
        let terminal = db
            .job_registry()
            .wait_terminal(job_id, Duration::from_secs(30))
            .unwrap();
        assert_eq!(terminal.state, crate::jobs::JobState::Failed);
        assert!(terminal
            .error
            .as_deref()
            .is_some_and(|error| error.contains("cancel")));
        assert_eq!(
            db.memory_governor()
                .usage(crate::memory::MemoryClass::Compaction),
            baseline
        );
        assert!(
            db.table("docs").unwrap().lock().schema().indexes.is_empty(),
            "pre-publication cancellation must keep the old schema"
        );
    }

    #[test]
    fn pending_index_job_reconstructs_and_resumes_after_reopen() {
        use std::time::Duration;

        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let job_id = {
            let db = Database::create(&path).unwrap();
            db.create_table("docs", plain_embedding_schema()).unwrap();
            put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
            let job_id = db.submit_create_index("docs", dense_definition()).unwrap();
            let pending = db.job_registry().get(job_id).unwrap();
            assert_eq!(pending.state, crate::jobs::JobState::Pending);
            assert!(pending.definition.is_some());
            job_id
        };

        let db = Arc::new(Database::open(&path).unwrap());
        db.resume_index_build(job_id).unwrap();
        let terminal = db
            .job_registry()
            .wait_terminal(job_id, Duration::from_secs(30))
            .unwrap();
        assert_eq!(terminal.state, crate::jobs::JobState::Succeeded);
        assert_eq!(
            db.table("docs")
                .unwrap()
                .lock()
                .schema()
                .indexes
                .iter()
                .find(|index| index.name == "idx_embed_ann")
                .and_then(|index| index.options.ann.as_ref())
                .map(|options| options.quantization),
            Some(AnnQuantization::Dense)
        );
    }

    #[test]
    fn dense_build_is_rejected_by_memory_admission_before_graph_allocation() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        let dim = 4_096u32;
        {
            let db = Database::create(&path).unwrap();
            let mut schema = plain_embedding_schema();
            schema.columns[1].ty = TypeId::Embedding { dim };
            db.create_table("docs", schema).unwrap();
            for id in 0..64 {
                let mut vector = vec![0.0; dim as usize];
                vector[id as usize] = 1.0;
                put_row(&db, "docs", id, vector);
            }
        }

        let db = Database::open_with_options(
            &path,
            crate::database::OpenOptions::default().with_memory_budget_bytes(2 * 1024 * 1024),
        )
        .unwrap();
        let baseline = db
            .memory_governor()
            .usage(crate::memory::MemoryClass::Compaction);
        let error = db
            .create_index("docs", dense_definition())
            .expect_err("oversized Dense graph must be denied");
        assert!(
            matches!(
                error,
                MongrelError::ResourceLimitExceeded {
                    resource: "index build memory",
                    ..
                }
            ),
            "unexpected admission error: {error:?}"
        );
        assert_eq!(
            db.memory_governor()
                .usage(crate::memory::MemoryClass::Compaction),
            baseline,
            "rejected build must not leak a reservation"
        );
        assert!(db.table("docs").unwrap().lock().schema().indexes.is_empty());
    }

    #[test]
    fn ddl_auth_fail_closed() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let db = Database::create_with_credentials(&path, "admin", "admin-pw").unwrap();
            db.create_table("docs", plain_embedding_schema()).unwrap();
            db.create_user("alice", "alice-pw").unwrap();
            db.create_role("reader").unwrap();
            db.grant_permission(
                "reader",
                crate::auth::Permission::Select {
                    table: "docs".into(),
                },
            )
            .unwrap();
            db.grant_role("alice", "reader").unwrap();
        }
        let db = Database::open_with_credentials(&path, "alice", "alice-pw").unwrap();
        let definition = IndexDef {
            name: "idx".into(),
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
        };
        let err = db.create_index("docs", definition).unwrap_err();
        assert!(
            matches!(err, MongrelError::PermissionDenied { .. }),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn cluster_replica_rejects_local_index_ddl() {
        let _lock = lock_tests();
        use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId};
        let dir = tempfile::tempdir().unwrap();
        let cluster_id = ClusterId::from_bytes([1; 16]);
        let node_id = NodeId::from_bytes([2; 16]);
        let database_id = DatabaseId::from_bytes([3; 16]);
        let db =
            Database::create_cluster_replica(dir.path(), cluster_id, node_id, database_id).unwrap();
        assert!(db.is_read_only_replica());
        let definition = IndexDef {
            name: "idx".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: Default::default(),
        };
        let err = db.create_index("docs", definition).unwrap_err();
        assert!(
            matches!(err, MongrelError::ReadOnlyReplica),
            "cluster replica must reject local index DDL: {err:?}"
        );
    }

    #[test]
    fn fault_before_publish_leaves_old_schema() {
        let _lock = lock_tests();
        // ScopedGuard clears the registry on drop so other serial tests never
        // observe a leftover armed hook.
        let _guard = mongreldb_fault::ScopedGuard::new(
            "index.publish.before",
            mongreldb_fault::Action::Fail,
        );
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", plain_embedding_schema()).unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);

        let definition = IndexDef {
            name: "idx_embed_ann".into(),
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
        };
        let err = db.create_index("docs", definition).unwrap_err();
        assert!(
            !matches!(err, MongrelError::NotFound(_)),
            "should fail at publish, not earlier: {err:?}"
        );
        let handle = db.table("docs").unwrap();
        let table = handle.lock();
        assert!(
            table.schema().indexes.is_empty(),
            "pre-publication fault must leave the old schema active"
        );
    }

    #[test]
    fn cancel_pre_publish_leaves_old_index() {
        let _lock = lock_tests();
        // Drive a job and cancel while Pending so publication never runs.
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", embedding_schema(AnnQuantization::BinarySign))
            .unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);

        // Submit without driving, then cancel.
        let job_id = db
            .job_registry()
            .submit(
                JobKind::IndexBuild,
                JobTarget {
                    table: "docs".into(),
                    index: Some("idx_embed_ann".into()),
                },
            )
            .unwrap();
        db.job_registry().cancel(job_id).unwrap();
        let record = db.job_registry().get(job_id).unwrap();
        assert_eq!(record.state, crate::jobs::JobState::Failed);

        // Schema still has the original BinarySign index.
        let handle = db.table("docs").unwrap();
        let table = handle.lock();
        let index = table
            .schema()
            .indexes
            .iter()
            .find(|index| index.name == "idx_embed_ann")
            .unwrap();
        assert_eq!(
            index
                .options
                .ann
                .as_ref()
                .map(|options| options.quantization),
            Some(AnnQuantization::BinarySign)
        );
    }

    #[test]
    fn replace_dense_with_binary_sign_search_works() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", embedding_schema(AnnQuantization::Dense))
            .unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
        put_row(&db, "docs", 2, vec![0.0, 1.0, 0.0, 0.0]);

        let new_def = IndexDef {
            name: "idx_embed_ann".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    m: 8,
                    ef_construction: 32,
                    ef_search: 16,
                    quantization: AnnQuantization::BinarySign,
                }),
                ..IndexOptions::default()
            },
        };
        db.replace_index("docs", "idx_embed_ann", new_def).unwrap();

        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        let hits = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: vec![1.0, 0.0, 0.0, 0.0],
                k: 1,
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(matches!(
            hits[0].score,
            RetrieverScore::AnnHammingDistance(_)
        ));
        assert!(!hits
            .iter()
            .any(|hit| matches!(hit.score, RetrieverScore::AnnCosineDistance(_))));
    }

    #[test]
    fn fault_after_publish_keeps_new_index() {
        let _lock = lock_tests();
        // `index.publish.after` fires only after the durable boundary; Fail
        // must not roll back the new schema (spec §4.7 / FND-006 after semantics).
        let _guard =
            mongreldb_fault::ScopedGuard::new("index.publish.after", mongreldb_fault::Action::Fail);
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path()).unwrap();
        db.create_table("docs", plain_embedding_schema()).unwrap();
        put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);

        let definition = IndexDef {
            name: "idx_embed_ann".into(),
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
        };
        // Publication may surface a post-fence error; the schema must still
        // carry the new Dense index.
        let _ = db.create_index("docs", definition);
        let handle = db.table("docs").unwrap();
        let table = handle.lock();
        let index = table
            .schema()
            .indexes
            .iter()
            .find(|index| index.name == "idx_embed_ann");
        assert!(
            index.is_some(),
            "post-publish fault must keep the new index active"
        );
        assert_eq!(
            index
                .unwrap()
                .options
                .ann
                .as_ref()
                .map(|options| options.quantization),
            Some(AnnQuantization::Dense)
        );
    }

    #[test]
    fn dense_plaintext_close_reopen_preserves_search() {
        let _lock = lock_tests();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();
        {
            let db = Database::create(&path).unwrap();
            db.create_table("docs", embedding_schema(AnnQuantization::Dense))
                .unwrap();
            put_row(&db, "docs", 1, vec![1.0, 0.0, 0.0, 0.0]);
            put_row(&db, "docs", 2, vec![0.0, 1.0, 0.0, 0.0]);
            // Force checkpoint materialization.
            let handle = db.table("docs").unwrap();
            let mut table = handle.lock();
            table.flush().unwrap();
            let _ = table.publish_read_generation().unwrap();
        }
        let db = Database::open(&path).unwrap();
        let handle = db.table("docs").unwrap();
        let mut table = handle.lock();
        let hits = table
            .retrieve(&Retriever::Ann {
                column_id: 2,
                query: vec![1.0, 0.0, 0.0, 0.0],
                k: 1,
            })
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert!(matches!(
            hits[0].score,
            RetrieverScore::AnnCosineDistance(d) if d.abs() < 1e-4
        ));
    }
}
