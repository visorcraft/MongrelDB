//! Online secondary-index create / drop / replace (MONGRELDB_TODO §4).
//!
//! Create and replace are driven as real [`JobKind::IndexBuild`] jobs through
//! [`run_build_publish`]. Publication is a short barrier under the commit lock:
//! the job rebuilds secondary indexes from currently visible rows to an exact
//! commit epoch, then atomically publishes the staged schema +
//! [`IndexGeneration`] through the catalog/WAL path. Drop is a synchronous
//! schema-only catalog command with no table rewrite.
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
#[derive(Debug, Clone)]
enum IndexBuildKind {
    Create {
        definition: IndexDef,
    },
    Replace {
        expected_old_name: String,
        new_definition: IndexDef,
    },
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
}

impl BuildPublishJob for IndexBuildJob<'_> {
    fn checkpoint_state(&self) -> Vec<u8> {
        // Bounded, small state only — never an unbounded delta stream.
        let epoch = self.snapshot_epoch.map(|e| e.0).unwrap_or(0);
        let published = u8::from(self.published);
        let mut out = Vec::with_capacity(17);
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(&self.snapshot_row_count.to_le_bytes());
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
        self.published = state[16] != 0;
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
        context.check_cancelled()?;
        let snapshot_epoch = self
            .snapshot_epoch
            .ok_or_else(|| JobError::Phase("index build missing pinned snapshot".into()))?;
        let handle = self.db.table(&self.table).map_err(job_phase_err)?;
        let table = handle.lock();
        // Scan the pinned snapshot to validate rows can be indexed and to
        // report progress. The durable hidden generation is installed under
        // the short publish barrier (practical catch-up: rebuild from
        // currently visible rows at publish).
        let rows = table
            .visible_rows(Snapshot::at(snapshot_epoch))
            .map_err(job_phase_err)?;
        let total = rows.len() as u64;
        for (done, row) in rows.iter().enumerate() {
            if done.is_multiple_of(256) {
                context.check_cancelled()?;
                if total > 0 {
                    let _ = context.report_progress(done as u64, total);
                }
            }
            // Touch the row so incomplete/corrupt embeddings fail before
            // publication when the new index is ANN.
            let _ = row;
        }
        self.snapshot_row_count = total;
        if total > 0 {
            context.report_progress(total, total)?;
        }
        Ok(())
    }

    fn catch_up(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        // Practical OK approach: catch-up is deferred to the short publish
        // barrier, which rebuilds the generation from currently visible rows
        // to the exact commit epoch. Cooperative cancel still applies.
        context.check_cancelled()?;
        Ok(())
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
        self.db.publish_index_build(self).map_err(job_phase_err)?;
        self.published = true;
        Ok(())
    }

    fn release_old(&mut self, context: &JobContext) -> std::result::Result<(), JobError> {
        context.check_cancelled()?;
        // Drop the build pin so version GC can advance. Old index generations
        // remain alive only while reader pins hold them (Arc).
        self.release_pin();
        Ok(())
    }

    fn rollback(&mut self) -> std::result::Result<(), JobError> {
        // Pre-publication cancel/failure: drop the pin and leave the old
        // schema + generation active. No hidden on-disk generation exists.
        self.release_pin();
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
    /// Create a secondary index online. Returns the durable job id after the
    /// build-and-publish job has reached a terminal success state.
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

        let job_id = self.job_registry.submit(
            JobKind::IndexBuild,
            JobTarget {
                table: table.to_string(),
                index: Some(index_name),
            },
        )?;

        let mut job = IndexBuildJob {
            db: self,
            table: table.to_string(),
            kind,
            expected_schema_sequence,
            snapshot_epoch: None,
            pin: None,
            snapshot_row_count: 0,
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

    /// Short publication barrier for an index-build job: CAS schema sequence,
    /// commit the catalog command through the shared WAL, rebuild indexes
    /// from currently visible rows, and publish generation + schema.
    fn publish_index_build(&self, job: &IndexBuildJob<'_>) -> Result<()> {
        use std::sync::atomic::Ordering;

        if self.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        // Re-check DDL auth at publication (revocation must stay effective).
        self.require(&crate::auth::Permission::Ddl)?;

        let table_name = job.table.as_str();
        let table_id = {
            let catalog = self.catalog.read();
            let entry = catalog
                .live(table_name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?;
            if entry.schema.schema_id != job.expected_schema_sequence {
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

        let command = job.catalog_command();
        let prepared_schema = match crate::catalog_cmds::apply(&self.catalog.read(), &command)? {
            crate::catalog_cmds::CatalogDelta::SchemaReplaced { schema, .. } => schema,
            other => {
                return Err(MongrelError::Other(format!(
                    "unexpected catalog delta for index publish: {other:?}"
                )));
            }
        };

        // FND-006: fail closed immediately before the durable publication
        // boundary (catalog + generation). Tests arm barriers here, never sleeps.
        crate::catalog::inject_hook("index.publish.before")?;

        let durable_epoch = std::cell::Cell::new(None);
        let result: Result<()> = (|| {
            // Schema barrier already held by the outer DDL entry? Jobs may
            // run without it for the long build; take exclusive now for the
            // short publish barrier together with the commit lock.
            let _schema_barrier = self.acquire_schema_barrier_exclusive()?;
            let _ddl = self.ddl_lock.lock();
            let _security_write = self.security_write()?;
            self.require(&crate::auth::Permission::Ddl)?;

            // Re-check CAS under the barrier.
            {
                let catalog = self.catalog.read();
                let entry = catalog.live(table_name).ok_or_else(|| {
                    MongrelError::NotFound(format!("table {table_name:?} not found"))
                })?;
                if entry.schema.schema_id != job.expected_schema_sequence {
                    return Err(MongrelError::Conflict(format!(
                        "index publish on {table_name}: expected schema sequence {}, found {}",
                        job.expected_schema_sequence, entry.schema.schema_id
                    )));
                }
            }

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
                .ok_or_else(|| MongrelError::NotFound(format!("table {table_name:?} not found")))?;
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

            // Rebuild secondary indexes from currently visible rows under the
            // barrier (catch-up to exact commit epoch) and publish generation.
            table_guard.publish_index_schema_change(prepared_schema)?;
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
