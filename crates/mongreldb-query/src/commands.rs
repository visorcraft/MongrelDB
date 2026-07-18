use crate::{
    ExternalBaseWrite, ExternalModuleIndex, ExternalModuleRegistry, ExternalWriteOp,
    MongrelProvider, MongrelQueryError, MongrelRecordBatchStream, MongrelSession,
    RegisteredSqlQuery, Result, SqlQueryPhase, SqlTestHookPoint,
};
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use mongreldb_core::constraint::{CheckConstraint as CoreCheckConstraint, CheckExpr};
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::procedure::{ProcedureCallOutput, ProcedureCallRow, StoredProcedure};
use mongreldb_core::rowid::RowId;
use mongreldb_core::schema::{
    AlterColumn, AnnOptions, AnnQuantization, ColumnDef as CoreColumnDef, ColumnFlags, DefaultExpr,
    IndexDef, IndexKind, IndexOptions, LearnedRangeOptions, MinHashOptions, Schema as CoreSchema,
    TypeId,
};
use mongreldb_core::Database;
use mongreldb_core::{
    ExternalTableDefinition, ExternalTableEntry, ExternalTriggerBaseWrite, ExternalTriggerBridge,
    ExternalTriggerWrite, ExternalTriggerWriteResult, ModuleArg, StoredTrigger, TriggerCell,
    TriggerDefinition, TriggerEvent, TriggerExpr, TriggerProgram, TriggerRaiseAction, TriggerStep,
    TriggerTarget, TriggerTiming, TriggerValue,
};
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, Assignment, AssignmentTarget,
    BinaryOperator, ColumnDef, ColumnOption, ConditionalStatements, CreateIndex, CreatePolicy,
    CreatePolicyCommand, CreatePolicyType, CreateTable, CreateTrigger, CreateView, DataType,
    Delete, DropPolicy, DropTrigger, Expr, FromTable, FunctionArg, FunctionArgExpr,
    FunctionArguments, Ident, IndexColumn, Insert, ObjectName, ObjectType, OnConflictAction,
    OnInsert, Owner, Query, RenameTableNameKind, Set, SetExpr, Statement, TableConstraint,
    TableFactor, TableObject, TableWithJoins, TransactionIsolationLevel, TransactionMode,
    TriggerEvent as SqlTriggerEvent, TriggerObject, TriggerObjectKind, TriggerPeriod, Truncate,
    UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) enum PendingSqlOp {
    Put {
        table: String,
        cells: Vec<(u16, Value)>,
    },
    Delete {
        table: String,
        row_id: RowId,
    },
    Truncate {
        table: String,
        changes: u64,
    },
    ExternalState {
        table: String,
        state: Vec<u8>,
        changes: u64,
    },
}

const PENDING_SQL_OPS_MEMORY_LIMIT: usize = 1_024;
const PENDING_SQL_OPS_MEMORY_BYTES_LIMIT: usize = 8 * 1024 * 1024;
const PENDING_SQL_OP_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const PENDING_SQL_OPS_TOTAL_BYTES_LIMIT: usize = 512 * 1024 * 1024;
const PENDING_SQL_SPILL_AAD_DOMAIN: &[u8] = b"mongreldb-pending-sql-spill-v1";
const ORDERED_DML_MAX_MATCHED_ROWS: usize = 100_000;
const ORDERED_DML_MATCHED_ROW_BYTES_LIMIT: usize = 256 * 1024 * 1024;
const ORDERED_DML_SORT_KEY_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const ORDERED_DML_SORT_RUN_ROWS: usize = 1_024;
const FOREIGN_KEY_CHECK_MAX_ROW_VISITS: usize = 10_000_000;
const FOREIGN_KEY_CHECK_KEY_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const FOREIGN_KEY_CHECK_TOTAL_KEY_BYTES_LIMIT: usize = 512 * 1024 * 1024;
const FOREIGN_KEY_CHECK_PARENT_KEY_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const FOREIGN_KEY_CHECK_MAX_VIOLATIONS: usize = 100_000;
const FOREIGN_KEY_CHECK_OUTPUT_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const CTAS_INPUT_BATCH_BYTES_LIMIT: usize = 256 * 1024 * 1024;
const CTAS_STAGING_ROWS_LIMIT: usize = 256;
const CTAS_STAGING_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const REBUILD_STAGING_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const INCREMENTAL_AGGREGATE_MAX_GROUPS: usize = 100_000;
const INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT: usize = 256 * 1024 * 1024;
const COMMAND_CHECKPOINT_ROWS: usize = 256;

async fn next_command_batch(
    stream: &mut MongrelRecordBatchStream,
    query: &RegisteredSqlQuery,
) -> Result<Option<RecordBatch>> {
    use futures::StreamExt;

    let item = tokio::select! {
        biased;
        _ = query.control().cancelled() => return match query.checkpoint() {
            Err(error) => Err(error),
            Ok(()) => Err(MongrelQueryError::InvalidQueryState(
                "query cancellation signal resolved without cancellation".into(),
            )),
        },
        item = stream.next() => item,
    };
    item.transpose()
        .map_err(|error| MongrelQueryError::DataFusion(error.to_string()))
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PendingSqlOpsCheckpoint {
    len: usize,
    spill_len: Option<u64>,
    memory_bytes: usize,
    total_bytes: usize,
}

#[derive(Default)]
pub(crate) struct PendingSqlOps {
    memory: Vec<PendingSqlOp>,
    spill: Option<PendingSqlSpill>,
    len: usize,
    memory_bytes: usize,
    total_bytes: usize,
}

struct PendingSqlSpill {
    file: std::fs::File,
    cipher: Aes256Gcm,
    nonce_prefix: [u8; 4],
    next_nonce: u64,
    frame_count: u64,
}

impl PendingSqlSpill {
    fn new() -> Result<Self> {
        let mut key = [0_u8; 32];
        getrandom::getrandom(&mut key)
            .map_err(|error| MongrelQueryError::InvalidQueryState(error.to_string()))?;
        let cipher = Aes256Gcm::new_from_slice(&key)
            .map_err(|error| MongrelQueryError::InvalidQueryState(error.to_string()))?;
        key.fill(0);
        let mut nonce_prefix = [0_u8; 4];
        getrandom::getrandom(&mut nonce_prefix)
            .map_err(|error| MongrelQueryError::InvalidQueryState(error.to_string()))?;
        Ok(Self {
            file: tempfile::tempfile().map_err(mongreldb_core::MongrelError::from)?,
            cipher,
            nonce_prefix,
            next_nonce: 0,
            frame_count: 0,
        })
    }

    fn append(&mut self, op: &PendingSqlOp) -> Result<()> {
        let plaintext = bincode::serialize(op).map_err(mongreldb_core::MongrelError::from)?;
        self.append_serialized(&plaintext)
    }

    fn append_serialized(&mut self, plaintext: &[u8]) -> Result<()> {
        if plaintext.len() > PENDING_SQL_OP_MAX_FRAME_BYTES {
            return Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "staged SQL operation bytes",
                    requested: plaintext.len(),
                    limit: PENDING_SQL_OP_MAX_FRAME_BYTES,
                },
            ));
        }
        let nonce_counter = self.next_nonce;
        let mut nonce = [0_u8; 12];
        nonce[..4].copy_from_slice(&self.nonce_prefix);
        nonce[4..].copy_from_slice(&nonce_counter.to_be_bytes());
        self.next_nonce = self.next_nonce.checked_add(1).ok_or_else(|| {
            MongrelQueryError::InvalidQueryState("SQL transaction spill nonce exhausted".into())
        })?;
        let ciphertext_len = plaintext.len().checked_add(16).ok_or_else(|| {
            MongrelQueryError::InvalidQueryState("SQL transaction spill frame too large".into())
        })?;
        let frame_len = u32::try_from(ciphertext_len).map_err(|_| {
            MongrelQueryError::InvalidQueryState("SQL transaction spill frame too large".into())
        })?;
        let aad = pending_sql_spill_aad(self.frame_count, frame_len, nonce_counter);
        let ciphertext = self
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| {
                MongrelQueryError::InvalidQueryState(
                    "failed to encrypt SQL transaction spill".into(),
                )
            })?;
        debug_assert_eq!(ciphertext.len(), ciphertext_len);
        self.file
            .seek(SeekFrom::End(0))
            .map_err(mongreldb_core::MongrelError::from)?;
        self.file
            .write_all(&frame_len.to_le_bytes())
            .and_then(|_| self.file.write_all(&nonce))
            .and_then(|_| self.file.write_all(&ciphertext))
            .map_err(mongreldb_core::MongrelError::from)?;
        self.frame_count = self.frame_count.checked_add(1).ok_or_else(|| {
            MongrelQueryError::InvalidQueryState(
                "SQL transaction spill frame count overflow".into(),
            )
        })?;
        Ok(())
    }
}

fn pending_sql_spill_aad(frame_index: u64, frame_len: u32, nonce_counter: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(PENDING_SQL_SPILL_AAD_DOMAIN.len() + 20);
    aad.extend_from_slice(PENDING_SQL_SPILL_AAD_DOMAIN);
    aad.extend_from_slice(&frame_index.to_be_bytes());
    aad.extend_from_slice(&frame_len.to_be_bytes());
    aad.extend_from_slice(&nonce_counter.to_be_bytes());
    aad
}

impl PendingSqlOps {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn push(&mut self, op: PendingSqlOp) -> Result<()> {
        let encoded = bincode::serialize(&op).map_err(mongreldb_core::MongrelError::from)?;
        if encoded.len() > PENDING_SQL_OP_MAX_FRAME_BYTES {
            return Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "staged SQL operation bytes",
                    requested: encoded.len(),
                    limit: PENDING_SQL_OP_MAX_FRAME_BYTES,
                },
            ));
        }
        let total_bytes = self.total_bytes.checked_add(encoded.len()).ok_or_else(|| {
            MongrelQueryError::InvalidQueryState(
                "SQL transaction staged byte count overflow".into(),
            )
        })?;
        if total_bytes > PENDING_SQL_OPS_TOTAL_BYTES_LIMIT {
            return Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "staged SQL transaction bytes",
                    requested: total_bytes,
                    limit: PENDING_SQL_OPS_TOTAL_BYTES_LIMIT,
                },
            ));
        }
        if let Some(spill) = self.spill.as_mut() {
            spill.append_serialized(&encoded)?;
        } else if self.memory.len() < PENDING_SQL_OPS_MEMORY_LIMIT
            && self.memory_bytes.saturating_add(encoded.len()) <= PENDING_SQL_OPS_MEMORY_BYTES_LIMIT
        {
            self.memory_bytes = self.memory_bytes.saturating_add(encoded.len());
            self.memory.push(op);
        } else {
            let mut spill = PendingSqlSpill::new()?;
            for staged in &self.memory {
                spill.append(staged)?;
            }
            spill.append_serialized(&encoded)?;
            self.memory.clear();
            self.memory_bytes = 0;
            self.spill = Some(spill);
        }
        self.len = self.len.saturating_add(1);
        self.total_bytes = total_bytes;
        Ok(())
    }

    pub(crate) fn extend(&mut self, ops: impl IntoIterator<Item = PendingSqlOp>) -> Result<()> {
        for op in ops {
            self.push(op)?;
        }
        Ok(())
    }

    pub(crate) fn from_vec(ops: Vec<PendingSqlOp>) -> Result<Self> {
        let mut staged = Self::default();
        staged.extend(ops)?;
        Ok(staged)
    }

    pub(crate) fn append_from(
        &mut self,
        other: &mut Self,
        query: &RegisteredSqlQuery,
    ) -> Result<()> {
        query.checkpoint()?;
        for (index, op) in other.reader()?.enumerate() {
            if index & 63 == 0 {
                query.checkpoint()?;
            }
            self.push(op?)?;
        }
        query.checkpoint()?;
        Ok(())
    }

    pub(crate) fn checkpoint(&mut self) -> Result<PendingSqlOpsCheckpoint> {
        let spill_len = if let Some(spill) = self.spill.as_mut() {
            let file = &mut spill.file;
            file.flush().map_err(mongreldb_core::MongrelError::from)?;
            Some(
                file.seek(SeekFrom::End(0))
                    .map_err(mongreldb_core::MongrelError::from)?,
            )
        } else {
            None
        };
        Ok(PendingSqlOpsCheckpoint {
            len: self.len,
            spill_len,
            memory_bytes: self.memory_bytes,
            total_bytes: self.total_bytes,
        })
    }

    pub(crate) fn truncate(&mut self, checkpoint: PendingSqlOpsCheckpoint) -> Result<()> {
        if let Some(spill_len) = checkpoint.spill_len {
            let spill = self.spill.as_mut().ok_or_else(|| {
                MongrelQueryError::InvalidQueryState(
                    "SQL transaction spill disappeared before rollback".into(),
                )
            })?;
            spill
                .file
                .set_len(spill_len)
                .map_err(mongreldb_core::MongrelError::from)?;
            spill
                .file
                .seek(SeekFrom::End(0))
                .map_err(mongreldb_core::MongrelError::from)?;
            spill.frame_count = u64::try_from(checkpoint.len).map_err(|_| {
                MongrelQueryError::InvalidQueryState(
                    "SQL transaction spill frame count overflow".into(),
                )
            })?;
            self.memory.clear();
            self.memory_bytes = 0;
        } else if self.spill.is_some() {
            let mut kept = Vec::with_capacity(checkpoint.len);
            let mut reader = self.reader()?;
            for _ in 0..checkpoint.len {
                kept.push(reader.next().ok_or_else(|| {
                    MongrelQueryError::InvalidQueryState(
                        "SQL transaction spill ended before rollback checkpoint".into(),
                    )
                })??);
            }
            self.spill = None;
            self.memory_bytes = kept
                .iter()
                .map(|op| bincode::serialized_size(op).unwrap_or(u64::MAX))
                .try_fold(0_usize, |total, size| {
                    usize::try_from(size)
                        .ok()
                        .and_then(|size| total.checked_add(size))
                })
                .ok_or_else(|| {
                    MongrelQueryError::InvalidQueryState(
                        "SQL transaction spill rollback size overflow".into(),
                    )
                })?;
            self.memory = kept;
        } else {
            self.memory.truncate(checkpoint.len);
            self.memory_bytes = checkpoint.memory_bytes;
        }
        self.len = checkpoint.len;
        self.total_bytes = checkpoint.total_bytes;
        Ok(())
    }

    pub(crate) fn reader(&mut self) -> Result<PendingSqlOpReader> {
        if let Some(spill) = self.spill.as_mut() {
            spill
                .file
                .flush()
                .map_err(mongreldb_core::MongrelError::from)?;
            let mut file = spill
                .file
                .try_clone()
                .map_err(mongreldb_core::MongrelError::from)?;
            file.seek(SeekFrom::Start(0))
                .map_err(mongreldb_core::MongrelError::from)?;
            Ok(PendingSqlOpReader::Spill {
                reader: Box::new(BufReader::new(file)),
                cipher: Box::new(spill.cipher.clone()),
                nonce_prefix: spill.nonce_prefix,
                frame_index: 0,
                previous_nonce: None,
                remaining: self.len,
            })
        } else {
            Ok(PendingSqlOpReader::Memory(self.memory.clone().into_iter()))
        }
    }
}

pub(crate) enum PendingSqlOpReader {
    Memory(std::vec::IntoIter<PendingSqlOp>),
    Spill {
        reader: Box<BufReader<std::fs::File>>,
        cipher: Box<Aes256Gcm>,
        nonce_prefix: [u8; 4],
        frame_index: u64,
        previous_nonce: Option<u64>,
        remaining: usize,
    },
}

impl Iterator for PendingSqlOpReader {
    type Item = Result<PendingSqlOp>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next().map(Ok),
            Self::Spill {
                reader,
                cipher,
                nonce_prefix,
                frame_index,
                previous_nonce,
                remaining,
            } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                let result = (|| {
                    let mut frame_len = [0_u8; 4];
                    reader
                        .read_exact(&mut frame_len)
                        .map_err(mongreldb_core::MongrelError::from)?;
                    let frame_len = u32::from_le_bytes(frame_len) as usize;
                    if frame_len > PENDING_SQL_OP_MAX_FRAME_BYTES + 16 {
                        return Err(MongrelQueryError::InvalidQueryState(
                            "SQL transaction spill frame exceeds limit".into(),
                        ));
                    }
                    let mut nonce = [0_u8; 12];
                    reader
                        .read_exact(&mut nonce)
                        .map_err(mongreldb_core::MongrelError::from)?;
                    if nonce[..4] != *nonce_prefix {
                        return Err(MongrelQueryError::InvalidQueryState(
                            "SQL transaction spill nonce prefix mismatch".into(),
                        ));
                    }
                    let nonce_counter = u64::from_be_bytes(nonce[4..].try_into().unwrap());
                    if previous_nonce.is_some_and(|previous| nonce_counter <= previous) {
                        return Err(MongrelQueryError::InvalidQueryState(
                            "SQL transaction spill nonce order invalid".into(),
                        ));
                    }
                    let mut ciphertext = vec![0_u8; frame_len];
                    reader
                        .read_exact(&mut ciphertext)
                        .map_err(mongreldb_core::MongrelError::from)?;
                    let aad = pending_sql_spill_aad(*frame_index, frame_len as u32, nonce_counter);
                    let plaintext = cipher
                        .decrypt(
                            Nonce::from_slice(&nonce),
                            Payload {
                                msg: &ciphertext,
                                aad: &aad,
                            },
                        )
                        .map_err(|_| {
                            MongrelQueryError::InvalidQueryState(
                                "SQL transaction spill authentication failed".into(),
                            )
                        })?;
                    *previous_nonce = Some(nonce_counter);
                    *frame_index = frame_index.checked_add(1).ok_or_else(|| {
                        MongrelQueryError::InvalidQueryState(
                            "SQL transaction spill frame count overflow".into(),
                        )
                    })?;
                    bincode::deserialize(&plaintext)
                        .map_err(mongreldb_core::MongrelError::from)
                        .map_err(Into::into)
                })();
                Some(result)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RollbackKind {
    Full,
    Savepoint,
}

fn enter_commit_fence(session: &MongrelSession, query: &RegisteredSqlQuery) -> Result<()> {
    session.fire_test_hook(SqlTestHookPoint::BeforeCommitFence);
    query.enter_commit_critical()?;
    session.fire_test_hook(SqlTestHookPoint::InsideCommitCritical);
    Ok(())
}

fn restore_failed_commit(session: &MongrelSession, ops: PendingSqlOps, transaction_open: bool) {
    if transaction_open {
        let mut transaction = session.transaction.lock();
        transaction.staged_ops = Some(ops);
        transaction.aborted = true;
    }
}

fn post_commit_result<T>(query: &RegisteredSqlQuery, result: Result<T>) -> Result<T> {
    result.map_err(|error| {
        if matches!(
            error,
            MongrelQueryError::CommitOutcome { .. } | MongrelQueryError::OutcomeUnknown { .. }
        ) {
            return error;
        }
        let status = query.status();
        if status.durable_outcome.last_commit_statement_index == Some(status.statement_index) {
            query.commit_outcome_error(error.to_string())
        } else {
            error
        }
    })
}

fn uncertain_fenced_error(
    session: &MongrelSession,
    query: &RegisteredSqlQuery,
    error: mongreldb_core::MongrelError,
) -> MongrelQueryError {
    match error {
        mongreldb_core::MongrelError::DurableCommit { epoch, message } => {
            query.record_commit(query.status().statement_index, epoch);
            let exit_error = query.exit_commit_critical().err();
            session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
            query.commit_outcome_error(match exit_error {
                Some(error) => format!("{message}; {error}"),
                None => message,
            })
        }
        error => {
            let exit_error = query.exit_commit_critical().err();
            query.outcome_unknown_error(exit_error.map_or_else(
                || error.to_string(),
                |exit_error| format!("{error}; {exit_error}"),
            ))
        }
    }
}

fn run_controlled_durable_with_optional_epoch<T>(
    session: &MongrelSession,
    query: &RegisteredSqlQuery,
    action: impl FnOnce(
        &mut dyn FnMut() -> mongreldb_core::Result<()>,
    ) -> mongreldb_core::Result<(T, Option<u64>)>,
) -> Result<T> {
    query.checkpoint()?;
    let fenced = std::cell::Cell::new(false);
    let mut before_publish = || {
        enter_commit_fence(session, query).map_err(query_error_to_core)?;
        fenced.set(true);
        Ok(())
    };
    let result = action(&mut before_publish);

    match result {
        Ok((value, None)) if !fenced.get() => {
            query.checkpoint()?;
            Ok(value)
        }
        Ok((value, Some(epoch))) if fenced.get() => {
            query.record_commit(query.status().statement_index, epoch);
            if let Err(error) = query.exit_commit_critical() {
                return Err(query.commit_outcome_error(error.to_string()));
            }
            session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
            Ok(value)
        }
        Ok((_value, epoch)) => {
            if query.status().phase == SqlQueryPhase::CommitCritical {
                let _ = query.exit_commit_critical();
            }
            Err(query.outcome_unknown_error(format!(
                "controlled durable write returned inconsistent fence/epoch state (fenced={}, epoch={epoch:?})",
                fenced.get()
            )))
        }
        Err(mongreldb_core::MongrelError::DurableCommit { epoch, message }) => {
            query.record_commit(query.status().statement_index, epoch);
            if query.status().phase == SqlQueryPhase::CommitCritical {
                if let Err(error) = query.exit_commit_critical() {
                    return Err(query.commit_outcome_error(format!("{message}; {error}")));
                }
            }
            session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
            query.checkpoint()?;
            Err(query.commit_outcome_error(message))
        }
        Err(error) if fenced.get() || query.status().phase == SqlQueryPhase::CommitCritical => {
            Err(uncertain_fenced_error(session, query, error))
        }
        Err(error) => {
            query.checkpoint()?;
            Err(error.into())
        }
    }
}

fn run_controlled_durable_with_epoch<T>(
    session: &MongrelSession,
    query: &RegisteredSqlQuery,
    action: impl FnOnce(
        &mut dyn FnMut() -> mongreldb_core::Result<()>,
    ) -> mongreldb_core::Result<(T, u64)>,
) -> Result<T> {
    run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
        action(before_publish).map(|(value, epoch)| (value, Some(epoch)))
    })
}

fn command_checkpoint(
    session: &MongrelSession,
    query: &RegisteredSqlQuery,
    index: usize,
) -> Result<()> {
    if index.is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
        session.fire_test_hook(SqlTestHookPoint::BeforeScanBatch);
        query.checkpoint()?;
    }
    Ok(())
}

pub(crate) async fn try_run_command(
    session: &MongrelSession,
    sql: &str,
    query: &RegisteredSqlQuery,
) -> Result<Option<Vec<RecordBatch>>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let lower = trimmed.to_ascii_lowercase();

    // WITH RECURSIVE — DataFusion v54's recursive CTE support is incomplete
    // (it mis-handles column aliases on the base case). Intercept and evaluate
    // iteratively using the standard naive recursive-evaluation algorithm.
    if lower.starts_with("with recursive ") {
        return try_recursive_cte(session, trimmed, query).await;
    }

    if lower.starts_with("refresh materialized view ") {
        let name = strip_identifier(&trimmed["refresh materialized view ".len()..])?;
        if name.chars().any(char::is_whitespace) {
            return Err(MongrelQueryError::Schema(
                "REFRESH MATERIALIZED VIEW requires one unqualified name".into(),
            ));
        }
        refresh_materialized_view(session, name, query).await?;
        return Ok(Some(Vec::new()));
    }

    if let Some(batch) = try_manual_command(session, trimmed, &lower, query)? {
        return Ok(Some(batch));
    }
    // SET TRANSACTION / SET SESSION CHARACTERISTICS drive the SQL transaction
    // isolation level (S1B-002). Intercept them here: `should_parse` does not
    // gate "set", and every other SET form must keep falling through to
    // DataFusion untouched.
    if lower.starts_with("set transaction ") || lower.starts_with("set session characteristics ") {
        return run_set_transaction(session, trimmed);
    }
    if !should_parse(trimmed) {
        return Ok(None);
    }

    let (parse_sql, ttl_clause) = extract_create_table_ttl(trimmed)?;
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &parse_sql)
        .map_err(|e| MongrelQueryError::Schema(format!("SQL parse error: {e}")))?;

    // Single-statement dispatch (multi-statement is handled by run() before
    // reaching this point).
    if statements.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "expected exactly one statement".into(),
        ));
    }

    let statement = statements.into_iter().next().ok_or_else(|| {
        MongrelQueryError::InvalidQueryState("SQL parser returned no statement".into())
    })?;
    if session.database.is_none()
        && matches!(
            &statement,
            Statement::Savepoint { .. }
                | Statement::ReleaseSavepoint { .. }
                | Statement::Rollback {
                    savepoint: Some(_),
                    ..
                }
        )
    {
        return Err(MongrelQueryError::NoSqlTransaction);
    }
    let Some(db) = session.database.as_ref() else {
        return Ok(None);
    };

    let out = match statement {
        Statement::CreateTable(create) => {
            require_ddl(session, db)?;
            create_table(session, db, &create, ttl_clause.as_ref(), query).await?;
            Vec::new()
        }
        Statement::CreateVirtualTable {
            name,
            if_not_exists,
            module_name,
            module_args,
        } => {
            require_ddl(session, db)?;
            create_virtual_table(
                session,
                db,
                name,
                if_not_exists,
                module_name.value,
                module_args.into_iter().map(|arg| arg.value).collect(),
                query,
            )?;
            Vec::new()
        }
        Statement::CreateTrigger(trigger) => {
            require_ddl(session, db)?;
            create_trigger(session, db, trigger, query)?;
            session.clear_cache();
            Vec::new()
        }
        Statement::DropTrigger(drop) => {
            require_ddl(session, db)?;
            drop_trigger(session, db, drop, query)?;
            session.clear_cache();
            Vec::new()
        }
        Statement::CreatePolicy(policy) => {
            create_policy(session, db, policy, query)?;
            Vec::new()
        }
        Statement::DropPolicy(policy) => {
            drop_policy(session, db, policy, query)?;
            Vec::new()
        }
        Statement::Drop {
            object_type,
            if_exists,
            names,
            table,
            ..
        } => {
            require_ddl(session, db)?;
            match object_type {
                ObjectType::Table => {
                    for name in names {
                        drop_table(session, db, &object_name(&name)?, if_exists, query)?;
                    }
                    Vec::new()
                }
                ObjectType::View => {
                    for name in names {
                        drop_view(session, db, &object_name(&name)?, if_exists, query)?;
                    }
                    Vec::new()
                }
                ObjectType::MaterializedView => {
                    for name in names {
                        drop_materialized_view(
                            session,
                            db,
                            &object_name(&name)?,
                            if_exists,
                            query,
                        )?;
                    }
                    Vec::new()
                }
                ObjectType::Index => {
                    drop_index(session, db, names, table, if_exists, query)?;
                    Vec::new()
                }
                _ => return Ok(None),
            }
        }
        Statement::AlterTable(alter) => {
            require_ddl(session, db)?;
            alter_table(session, db, alter, query)?;
            Vec::new()
        }
        Statement::CreateIndex(index) => {
            require_ddl(session, db)?;
            create_index(session, db, index, query)?;
            Vec::new()
        }
        Statement::CreateView(view) => {
            require_ddl(session, db)?;
            create_view(session, view, query).await?;
            Vec::new()
        }
        Statement::Insert(insert) => {
            insert_rows(session, db, insert, query)?;
            Vec::new()
        }
        Statement::Update(update) => {
            update_rows(session, db, update, query).await?;
            Vec::new()
        }
        Statement::Delete(delete) => {
            delete_rows(session, db, delete, query).await?;
            Vec::new()
        }
        Statement::Truncate(truncate) => {
            truncate_tables(session, db, truncate, query)?;
            Vec::new()
        }
        Statement::StartTransaction { modes, .. } => {
            let requested = sql_isolation_from_modes(&modes)?;
            let mut transaction = session.transaction.lock();
            if transaction.staged_ops.is_some() {
                return Err(MongrelQueryError::Schema(
                    "a SQL transaction is already open".into(),
                ));
            }
            transaction.staged_ops = Some(PendingSqlOps::default());
            transaction.aborted = false;
            transaction.savepoints.clear();
            transaction.predicate_reads.clear();
            transaction.for_update_txn_id = None;
            // Explicit mode > pending SET TRANSACTION > session default. The
            // resolved level stays recorded so a mid-transaction SET
            // TRANSACTION can replace it and COMMIT can read it back.
            let inherited = transaction.isolation.take();
            transaction.isolation = Some(
                requested
                    .or(inherited)
                    .unwrap_or_else(|| *session.session_isolation.read()),
            );
            Vec::new()
        }
        Statement::Commit { chain, .. } => {
            if chain {
                return Err(MongrelQueryError::Schema(
                    "COMMIT AND CHAIN is not supported".into(),
                ));
            }
            let mut empty_ops = PendingSqlOps::default();
            let (mut ops, transaction_open, changes, external_tables) = {
                let mut transaction = session.transaction.lock();
                if transaction.aborted {
                    return Err(MongrelQueryError::TransactionAborted);
                }
                let transaction_open = transaction.staged_ops.is_some();
                let preparation = {
                    let ops = transaction.staged_ops.as_mut().unwrap_or(&mut empty_ops);
                    session.fire_test_hook(SqlTestHookPoint::BeforeScanBatch);
                    (|| {
                        let changes = logical_changes_spooled(ops, query)?;
                        let external_tables = external_tables_to_refresh_spooled(db, ops, query)?;
                        Ok((changes, external_tables))
                    })()
                };
                let (changes, external_tables) = match preparation {
                    Ok(preparation) => preparation,
                    Err(error) => {
                        transaction.aborted = transaction_open;
                        return Err(error);
                    }
                };
                (
                    transaction.staged_ops.take().unwrap_or_default(),
                    transaction_open,
                    changes,
                    external_tables,
                )
            };
            let epoch = match apply_ops(session, db, &mut ops, query) {
                Ok(epoch) => epoch,
                Err(error) => {
                    if matches!(&error, MongrelQueryError::OutcomeUnknown { .. }) {
                        // The commit fence was crossed, so replaying these
                        // staged operations could duplicate a commit whose
                        // acknowledgement was lost. Discard every savepoint
                        // and let the statement guard leave the transaction
                        // aborted until a full ROLLBACK.
                        session.clear_sql_transaction();
                        return Err(error);
                    }
                    if matches!(
                        &error,
                        MongrelQueryError::CommitOutcome {
                            committed: true,
                            ..
                        }
                    ) {
                        session.clear_sql_transaction();
                        if let Err(refresh_error) = sync_committed_statement(
                            session,
                            db,
                            &external_tables,
                            changes,
                            None,
                            query,
                        ) {
                            if matches!(
                                &refresh_error,
                                MongrelQueryError::QueryCancelled { .. }
                                    | MongrelQueryError::DeadlineExceeded { .. }
                            ) {
                                return Err(refresh_error);
                            }
                            return Err(query.commit_outcome_error(format!(
                                "{error}; external table refresh failed: {refresh_error}"
                            )));
                        }
                        return Err(error);
                    }
                    restore_failed_commit(session, ops, transaction_open);
                    return Err(error);
                }
            };
            let committed = epoch.is_some();
            if let Some(epoch) = epoch {
                query.record_commit_with_ts(
                    query.status().statement_index,
                    epoch.0,
                    db.commit_ts_for_epoch(epoch),
                );
                if let Err(error) = query.exit_commit_critical() {
                    return Err(query.commit_outcome_error(error.to_string()));
                }
                session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
            }
            session.clear_sql_transaction();
            if let Err(error) =
                sync_committed_statement(session, db, &external_tables, changes, None, query)
            {
                if !committed {
                    return Err(error);
                }
                if matches!(
                    &error,
                    MongrelQueryError::QueryCancelled { .. }
                        | MongrelQueryError::DeadlineExceeded { .. }
                ) {
                    return Err(error);
                }
                return Err(query.commit_outcome_error(error.to_string()));
            }
            query.checkpoint()?;
            Vec::new()
        }
        Statement::Rollback { chain, savepoint } => {
            if chain {
                return Err(MongrelQueryError::Schema(
                    "ROLLBACK AND CHAIN is not supported".into(),
                ));
            }
            if let Some(name) = savepoint {
                let name = savepoint_name(&name);
                let mut transaction = session.transaction.lock();
                if transaction.staged_ops.is_none() {
                    return Err(MongrelQueryError::NoSqlTransaction);
                }
                let pos = transaction
                    .savepoints
                    .iter()
                    .rposition(|(candidate, _)| candidate == &name)
                    .ok_or_else(|| MongrelQueryError::SavepointNotFound { name: name.clone() })?;
                let checkpoint = transaction.savepoints[pos].1;
                if let Some(ops) = transaction.staged_ops.as_mut() {
                    ops.truncate(checkpoint)?;
                }
                transaction.savepoints.truncate(pos + 1);
                transaction.aborted = false;
            } else {
                session.clear_sql_transaction();
            }
            Vec::new()
        }
        Statement::Savepoint { name } => {
            let name = savepoint_name(&name);
            let mut transaction = session.transaction.lock();
            let checkpoint = transaction
                .staged_ops
                .as_mut()
                .ok_or(MongrelQueryError::NoSqlTransaction)?
                .checkpoint()?;
            transaction.savepoints.push((name, checkpoint));
            Vec::new()
        }
        Statement::ReleaseSavepoint { name } => {
            let name = savepoint_name(&name);
            let mut transaction = session.transaction.lock();
            if transaction.staged_ops.is_none() {
                return Err(MongrelQueryError::NoSqlTransaction);
            }
            let pos = transaction
                .savepoints
                .iter()
                .rposition(|(candidate, _)| candidate == &name)
                .ok_or_else(|| MongrelQueryError::SavepointNotFound { name: name.clone() })?;
            transaction.savepoints.truncate(pos);
            Vec::new()
        }
        Statement::Analyze(_) => {
            require_ddl(session, db)?;
            analyze_all(session, db, query)?;
            session.clear_cache();
            Vec::new()
        }
        Statement::Vacuum(_) => {
            require_ddl(session, db)?;
            compact_all(session, db, query)?;
            session.clear_cache();
            Vec::new()
        }
        _ => return Ok(None),
    };

    Ok(Some(out))
}

/// `SET TRANSACTION ...` / `SET SESSION CHARACTERISTICS AS TRANSACTION ...`:
/// drive the SQL transaction isolation level (S1B-002). SET TRANSACTION
/// applies to the open transaction (or pends for the next `BEGIN`); SET
/// SESSION CHARACTERISTICS sets the session default for later transactions.
/// Every other SET form falls through to DataFusion untouched.
fn run_set_transaction(session: &MongrelSession, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql)
        .map_err(|e| MongrelQueryError::Schema(format!("SQL parse error: {e}")))?;
    if statements.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "expected exactly one statement".into(),
        ));
    }
    let Some(Statement::Set(Set::SetTransaction {
        modes,
        snapshot,
        session: session_wide,
    })) = statements.first()
    else {
        return Err(MongrelQueryError::Schema(
            "expected SET TRANSACTION or SET SESSION CHARACTERISTICS AS TRANSACTION".into(),
        ));
    };
    if snapshot.is_some() {
        return Err(MongrelQueryError::Schema(
            "SET TRANSACTION SNAPSHOT is not supported".into(),
        ));
    }
    if let Some(level) = sql_isolation_from_modes(modes)? {
        session.set_sql_isolation(level, *session_wide);
    }
    Ok(Some(Vec::new()))
}

/// Map sqlparser transaction modes onto a core isolation level. Access
/// modes and levels core does not implement are rejected rather than silently
/// ignored (fail closed): `READ UNCOMMITTED` has no core counterpart, and a
/// `READ ONLY` request would not be enforced by the staged-commit model.
fn sql_isolation_from_modes(
    modes: &[TransactionMode],
) -> Result<Option<mongreldb_core::IsolationLevel>> {
    let mut level = None;
    for mode in modes {
        match mode {
            TransactionMode::IsolationLevel(isolation) => {
                if level.is_some() {
                    return Err(MongrelQueryError::Schema(
                        "a transaction statement carries at most one isolation level".into(),
                    ));
                }
                level = Some(match isolation {
                    TransactionIsolationLevel::ReadCommitted => {
                        mongreldb_core::IsolationLevel::ReadCommitted
                    }
                    TransactionIsolationLevel::RepeatableRead
                    | TransactionIsolationLevel::Snapshot => {
                        // Core's historical `Snapshot` name is a deprecated
                        // RepeatableRead alias (`IsolationLevel::canonical`).
                        mongreldb_core::IsolationLevel::RepeatableRead
                    }
                    TransactionIsolationLevel::Serializable => {
                        mongreldb_core::IsolationLevel::Serializable
                    }
                    TransactionIsolationLevel::ReadUncommitted => {
                        return Err(MongrelQueryError::Schema(
                            "READ UNCOMMITTED is not supported; core implements READ COMMITTED, REPEATABLE READ, and SERIALIZABLE".into(),
                        ));
                    }
                });
            }
            TransactionMode::AccessMode(_) => {
                return Err(MongrelQueryError::Schema(
                    "transaction access modes (READ ONLY / READ WRITE) are not supported".into(),
                ));
            }
        }
    }
    Ok(level)
}

/// Whether the session's principal is the database's own stored principal,
/// compared by catalog identity. The serializable commit path begins its core
/// transaction with `Database::begin_with_isolation`, which takes the
/// database principal; it must not silently commit under a different
/// identity than the session's.
fn session_principal_matches_database(session: &MongrelSession, db: &Database) -> bool {
    match (session.principal(), db.principal_snapshot()) {
        (None, None) => true,
        (Some(session), Some(database)) => {
            session.user_id == database.user_id && session.created_epoch == database.created_epoch
        }
        _ => false,
    }
}

#[derive(Debug, Clone)]
struct ParsedTtlClause {
    column: String,
    duration_nanos: u64,
}

/// Strip MongrelDB's `TTL_COLUMN <column> TTL '<duration>'` suffix before
/// handing the standard portion to sqlparser.
fn extract_create_table_ttl(sql: &str) -> Result<(String, Option<ParsedTtlClause>)> {
    if !sql
        .trim_start()
        .to_ascii_lowercase()
        .starts_with("create table")
    {
        return Ok((sql.to_string(), None));
    }
    let Some(close) = sql.rfind(')') else {
        return Ok((sql.to_string(), None));
    };
    let tail = sql[close + 1..].trim().trim_end_matches(';').trim();
    if tail.is_empty() {
        return Ok((sql.to_string(), None));
    }
    let lower_tail = tail.to_ascii_lowercase();
    let Some(rest) = lower_tail.strip_prefix("ttl_column ") else {
        return Ok((sql.to_string(), None));
    };
    let Some(ttl_pos) = rest.find(" ttl ") else {
        return Err(MongrelQueryError::Schema(
            "TTL syntax: TTL_COLUMN <timestamp_column> TTL '<duration>'".into(),
        ));
    };
    let prefix_len = "ttl_column ".len();
    let column = tail[prefix_len..prefix_len + ttl_pos]
        .trim()
        .trim_matches('"')
        .to_string();
    if column.is_empty() {
        return Err(MongrelQueryError::Schema(
            "TTL_COLUMN requires a column name".into(),
        ));
    }
    let duration = tail[prefix_len + ttl_pos + " ttl ".len()..].trim();
    let duration_nanos = parse_ttl_duration(duration)?;
    Ok((
        sql[..=close].to_string(),
        Some(ParsedTtlClause {
            column,
            duration_nanos,
        }),
    ))
}

fn parse_ttl_duration(input: &str) -> Result<u64> {
    let literal = input.trim().trim_matches('\'').trim();
    let mut parts = literal.split_whitespace();
    let amount: u64 = parts
        .next()
        .ok_or_else(|| MongrelQueryError::Schema("TTL duration is empty".into()))?
        .parse()
        .map_err(|_| MongrelQueryError::Schema(format!("invalid TTL duration {input}")))?;
    let unit = parts
        .next()
        .ok_or_else(|| MongrelQueryError::Schema("TTL duration requires a unit".into()))?
        .to_ascii_lowercase();
    if parts.next().is_some() || amount == 0 {
        return Err(MongrelQueryError::Schema(format!(
            "invalid TTL duration {input}"
        )));
    }
    let multiplier = match unit.as_str() {
        "ns" | "nanosecond" | "nanoseconds" => 1,
        "us" | "microsecond" | "microseconds" => 1_000,
        "ms" | "millisecond" | "milliseconds" => 1_000_000,
        "s" | "second" | "seconds" => 1_000_000_000,
        "m" | "minute" | "minutes" => 60_000_000_000,
        "h" | "hour" | "hours" => 3_600_000_000_000,
        "d" | "day" | "days" => 86_400_000_000_000,
        "w" | "week" | "weeks" => 604_800_000_000_000,
        _ => {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported TTL duration unit {unit}"
            )))
        }
    };
    amount
        .checked_mul(multiplier)
        .filter(|value| *value <= i64::MAX as u64)
        .ok_or_else(|| MongrelQueryError::Schema("TTL duration is too large".into()))
}

fn should_parse(sql: &str) -> bool {
    let Ok(tokens) = Tokenizer::new(&GenericDialect {}, sql).tokenize() else {
        return false;
    };
    let Some(Token::Word(word)) = tokens
        .iter()
        .find(|token| !matches!(token, Token::Whitespace(_)))
    else {
        return false;
    };
    matches!(
        word.value.to_ascii_lowercase().as_str(),
        "create"
            | "drop"
            | "alter"
            | "insert"
            | "replace"
            | "update"
            | "delete"
            | "truncate"
            | "begin"
            | "start"
            | "commit"
            | "rollback"
            | "savepoint"
            | "release"
            | "analyze"
            | "vacuum"
    )
}

pub(crate) fn rollback_kind(sql: &str) -> Option<RollbackKind> {
    let statements = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }
    match &statements[0] {
        Statement::Rollback {
            savepoint: Some(_), ..
        } => Some(RollbackKind::Savepoint),
        Statement::Rollback {
            savepoint: None, ..
        } => Some(RollbackKind::Full),
        _ => None,
    }
}

fn savepoint_name(name: &Ident) -> String {
    if name.quote_style.is_some() {
        name.value.clone()
    } else {
        name.value.to_ascii_lowercase()
    }
}

fn require_ddl(session: &MongrelSession, db: &Arc<Database>) -> Result<()> {
    db.require_for(
        session.principal().as_ref(),
        &mongreldb_core::Permission::Ddl,
    )?;
    Ok(())
}

fn try_manual_command(
    session: &MongrelSession,
    sql: &str,
    lower: &str,
    query: &RegisteredSqlQuery,
) -> Result<Option<Vec<RecordBatch>>> {
    try_manual_command_body(session, sql, lower, query)
}

fn try_manual_command_body(
    session: &MongrelSession,
    sql: &str,
    lower: &str,
    query: &RegisteredSqlQuery,
) -> Result<Option<Vec<RecordBatch>>> {
    let admin_command = [
        "create user ",
        "alter user ",
        "drop user ",
        "create role ",
        "drop role ",
        "grant ",
        "revoke ",
        "create mask ",
        "drop mask ",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix));
    if admin_command {
        if let Some(db) = &session.database {
            db.require_for(
                session.principal().as_ref(),
                &mongreldb_core::Permission::Admin,
            )?;
        }
    }
    if lower.starts_with("attach ") || lower.starts_with("detach ") {
        if let Some(db) = &session.database {
            db.require_for(
                session.principal().as_ref(),
                &mongreldb_core::Permission::Ddl,
            )?;
        }
    }

    // ATTACH/DETACH don't require a primary Database — they mount external ones.
    if lower.starts_with("attach ") {
        return attach_database(session, sql);
    }
    if lower.starts_with("detach ") {
        return detach_database(session, sql);
    }

    // NOTIFY channel [, payload] — publish a notification on a named channel.
    if let Some(rest) = lower.strip_prefix("notify ") {
        let original_rest = &sql["notify ".len()..];
        let (channel, payload) = parse_notify_args(rest, original_rest)?;
        if let Some(db) = &session.database {
            db.notify(&channel, payload);
        }
        return Ok(Some(Vec::new()));
    }
    // LISTEN channel — accepted as a no-op at the SQL layer (subscribers
    // connect via /events SSE or Database::subscribe_changes).
    if let Some(_rest) = lower.strip_prefix("listen ") {
        return Ok(Some(Vec::new()));
    }
    if let Some(_rest) = lower.strip_prefix("unlisten ") {
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("create mask ") {
        create_mask(session, sql, query)?;
        return Ok(Some(Vec::new()));
    }
    if lower.starts_with("drop mask ") {
        drop_mask(session, sql, query)?;
        return Ok(Some(Vec::new()));
    }

    // User/role/credentials management — operate on the catalog.
    if let Some(_rest) = lower.strip_prefix("create user ") {
        let original_rest = &sql["create user ".len()..];
        let trimmed = original_rest.trim().trim_end_matches(';').trim();
        let (name, password) =
            if let Some(idx) = trimmed.to_ascii_lowercase().find(" with password ") {
                let name = trimmed[..idx]
                    .trim()
                    .trim_matches(|c| c == '"' || c == '\'')
                    .to_string();
                let pw_part = &trimmed[idx + " with password ".len()..];
                let pw = pw_part.trim().trim_matches('\'').to_string();
                (name, pw)
            } else {
                (
                    trimmed.trim_matches(|c| c == '"' || c == '\'').to_string(),
                    String::new(),
                )
            };
        if name.is_empty() {
            return Err(MongrelQueryError::Schema(
                "CREATE USER requires a name".into(),
            ));
        }
        let Some(db) = &session.database else {
            return Ok(Some(Vec::new()));
        };
        let hash =
            mongreldb_core::auth::hash_password(&password).map_err(MongrelQueryError::Schema)?;
        run_controlled_durable_with_epoch(session, query, |before_publish| {
            let user = db.create_user_with_password_hash_controlled(&name, hash, before_publish)?;
            let epoch = user.created_epoch;
            Ok(((), epoch))
        })?;
        return Ok(Some(Vec::new()));
    }
    if let Some(_rest) = lower.strip_prefix("alter user ") {
        let original_rest = &sql["alter user ".len()..];
        let trimmed = original_rest.trim().trim_end_matches(';').trim();
        let lower_trimmed = trimmed.to_ascii_lowercase();
        let Some(db) = &session.database else {
            return Ok(Some(Vec::new()));
        };
        // ALTER USER <name> PASSWORD '<new>'
        if let Some(idx) = lower_trimmed.find(" password ") {
            let name = trimmed[..idx]
                .trim()
                .trim_matches(|c| c == '"' || c == '\'')
                .to_string();
            let pw = trimmed[idx + " password ".len()..]
                .trim()
                .trim_matches('\'')
                .to_string();
            let hash =
                mongreldb_core::auth::hash_password(&pw).map_err(MongrelQueryError::Schema)?;
            run_controlled_durable_with_epoch(session, query, |before_publish| {
                let epoch =
                    db.alter_user_password_hash_with_epoch_controlled(&name, hash, before_publish)?;
                Ok(((), epoch.0))
            })?;
            return Ok(Some(Vec::new()));
        }
        // ALTER USER <name> NOT ADMIN
        if lower_trimmed.ends_with(" not admin") {
            let name = trimmed[..trimmed.len() - " not admin".len()]
                .trim()
                .trim_matches(|c| c == '"' || c == '\'')
                .to_string();
            run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
                let epoch =
                    db.set_user_admin_with_epoch_controlled(&name, false, before_publish)?;
                Ok(((), epoch.map(|epoch| epoch.0)))
            })?;
            return Ok(Some(Vec::new()));
        }
        // ALTER USER <name> ADMIN
        if lower_trimmed.ends_with(" admin") {
            let name = trimmed[..trimmed.len() - " admin".len()]
                .trim()
                .trim_matches(|c| c == '"' || c == '\'')
                .to_string();
            run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
                let epoch = db.set_user_admin_with_epoch_controlled(&name, true, before_publish)?;
                Ok(((), epoch.map(|epoch| epoch.0)))
            })?;
            return Ok(Some(Vec::new()));
        }
        return Err(MongrelQueryError::Schema(
            "ALTER USER requires: ALTER USER <name> PASSWORD '<new>' | ADMIN | NOT ADMIN".into(),
        ));
    }
    if let Some(rest) = lower.strip_prefix("drop user ") {
        let name = rest.trim().trim_end_matches(';').trim().to_string();
        if let Some(db) = &session.database {
            run_controlled_durable_with_epoch(session, query, |before_publish| {
                let epoch = db.drop_user_with_epoch_controlled(&name, before_publish)?;
                Ok(((), epoch.0))
            })?;
        }
        return Ok(Some(Vec::new()));
    }
    if let Some(rest) = lower.strip_prefix("create role ") {
        let name = rest.trim().trim_end_matches(';').trim().to_string();
        if let Some(db) = &session.database {
            run_controlled_durable_with_epoch(session, query, |before_publish| {
                let role = db.create_role_controlled(&name, before_publish)?;
                let epoch = role.created_epoch;
                Ok(((), epoch))
            })?;
        }
        return Ok(Some(Vec::new()));
    }
    if let Some(rest) = lower.strip_prefix("drop role ") {
        let name = rest.trim().trim_end_matches(';').trim().to_string();
        if let Some(db) = &session.database {
            run_controlled_durable_with_epoch(session, query, |before_publish| {
                let epoch = db.drop_role_with_epoch_controlled(&name, before_publish)?;
                Ok(((), epoch.0))
            })?;
        }
        return Ok(Some(Vec::new()));
    }
    if lower.starts_with("grant ") || lower.starts_with("revoke ") {
        let is_grant = lower.starts_with("grant ");
        let rest = &lower[if is_grant { 6 } else { 7 }..];
        let original_rest = &sql[if is_grant { 6 } else { 7 }..];
        // GRANT <perm> ON <table> TO <role>  |  GRANT <role> TO <user>
        // REVOKE <perm> ON <table> FROM <role> | REVOKE <role> FROM <user>
        let sep = if is_grant { " to " } else { " from " };
        let Some(sep_idx) = rest.find(sep) else {
            return Err(MongrelQueryError::Schema(format!(
                "{} requires ... {} <target>",
                if is_grant { "GRANT" } else { "REVOKE" },
                if is_grant { "TO" } else { "FROM" }
            )));
        };
        let left = &original_rest[..sep_idx].trim();
        let target = original_rest[sep_idx + sep.len()..]
            .trim()
            .trim_end_matches(';')
            .trim();
        let Some(db) = &session.database else {
            return Ok(Some(Vec::new()));
        };
        if left.to_ascii_lowercase().contains(" on ") {
            // GRANT SELECT ON table TO role
            let on_idx = left.to_ascii_lowercase().find(" on ").ok_or_else(|| {
                MongrelQueryError::InvalidQueryState(
                    "GRANT/REVOKE permission separator disappeared".into(),
                )
            })?;
            let perm_text = left[..on_idx].trim();
            let table = &original_rest[..sep_idx][on_idx + 4..]
                .trim()
                .trim_matches(|c| c == '"' || c == '\'');
            let table = table.trim_end_matches(';').trim();
            let permission = parse_grant_permission(db, perm_text, table)?;
            if is_grant {
                run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
                    let epoch = db.grant_permission_with_epoch_controlled(
                        target,
                        permission,
                        before_publish,
                    )?;
                    Ok(((), epoch.map(|epoch| epoch.0)))
                })?;
            } else {
                run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
                    let epoch = db.revoke_permission_with_epoch_controlled(
                        target,
                        permission,
                        before_publish,
                    )?;
                    Ok(((), epoch.map(|epoch| epoch.0)))
                })?;
            }
        } else {
            // GRANT role TO user
            if is_grant {
                run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
                    let epoch =
                        db.grant_role_with_epoch_controlled(target, left.trim(), before_publish)?;
                    Ok(((), epoch.map(|epoch| epoch.0)))
                })?;
            } else {
                run_controlled_durable_with_optional_epoch(session, query, |before_publish| {
                    let epoch =
                        db.revoke_role_with_epoch_controlled(target, left.trim(), before_publish)?;
                    Ok(((), epoch.map(|epoch| epoch.0)))
                })?;
            }
        }
        return Ok(Some(Vec::new()));
    }
    if lower == "show users" || lower == "show user" {
        if let Some(db) = &session.database {
            let names: Vec<String> = db.users().into_iter().map(|u| u.username).collect();
            return Ok(Some(vec![strings_batch("username", names)?]));
        }
        return Ok(Some(Vec::new()));
    }
    if lower == "show roles" || lower == "show role" {
        if let Some(db) = &session.database {
            let names: Vec<String> = db.roles().into_iter().map(|r| r.name).collect();
            return Ok(Some(vec![strings_batch("role_name", names)?]));
        }
        return Ok(Some(Vec::new()));
    }

    let Some(db) = session.database.as_ref() else {
        return Ok(None);
    };

    if lower == "show tables" || lower == "show table" {
        let mut names = db.table_names();
        names.sort();
        return Ok(Some(vec![strings_batch("table_name", names)?]));
    }

    if lower.starts_with("create virtual table ") {
        create_virtual_table_manual(session, db, sql, query)?;
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("create trigger if not exists ") {
        create_trigger_if_not_exists_manual(session, db, sql, query)?;
        return Ok(Some(Vec::new()));
    }

    if lower == "show procedures" || lower == "show procedure" {
        let mut names: Vec<String> = db.procedures().into_iter().map(|p| p.name).collect();
        names.sort();
        return Ok(Some(vec![strings_batch("procedure_name", names)?]));
    }

    if let Some(name) = lower
        .strip_prefix("describe procedure ")
        .or_else(|| lower.strip_prefix("desc procedure "))
    {
        let name = strip_identifier(name)?;
        let procedure = db
            .procedure(name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("procedure {name:?} not found")))?;
        return Ok(Some(vec![json_batch(
            "procedure_json",
            vec![serde_json::to_string(&procedure)
                .map_err(|e| MongrelQueryError::Schema(e.to_string()))?],
        )?]));
    }

    if lower.starts_with("create or replace procedure ") {
        let (name, json) = parse_procedure_json(sql, lower, "create or replace procedure ")?;
        let procedure = procedure_from_json(name, json)?;
        run_controlled_durable_with_epoch(session, query, |before_publish| {
            let procedure = db.create_or_replace_procedure_controlled(procedure, before_publish)?;
            let epoch = procedure.updated_epoch;
            Ok(((), epoch))
        })?;
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("create procedure ") {
        let (name, json) = parse_procedure_json(sql, lower, "create procedure ")?;
        let procedure = procedure_from_json(name, json)?;
        run_controlled_durable_with_epoch(session, query, |before_publish| {
            let procedure = db.create_procedure_controlled(procedure, before_publish)?;
            let epoch = procedure.updated_epoch;
            Ok(((), epoch))
        })?;
        return Ok(Some(Vec::new()));
    }

    if let Some(name) = lower.strip_prefix("drop procedure ") {
        let name = strip_identifier(name)?;
        run_controlled_durable_with_epoch(session, query, |before_publish| {
            let epoch = db.drop_procedure_with_epoch_controlled(name, before_publish)?;
            Ok(((), epoch.0))
        })?;
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("call ") {
        let (name, args) = parse_call_json(sql, lower)?;
        let mut fence_error = None;
        let mut fenced = false;
        let result = db.call_procedure_as_controlled(
            name,
            args,
            session.principal().as_ref(),
            query.control(),
            || match enter_commit_fence(session, query) {
                Ok(()) => {
                    fenced = true;
                    true
                }
                Err(error) => {
                    fence_error = Some(error);
                    false
                }
            },
        );
        if let Some(error) = fence_error {
            return Err(error);
        }
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if fenced {
                    return Err(uncertain_fenced_error(session, query, error));
                } else {
                    query.checkpoint()?;
                }
                return Err(error.into());
            }
        };
        if let Some(epoch) = result.epoch {
            query.record_commit(query.status().statement_index, epoch);
            if let Err(error) = query.exit_commit_critical() {
                return Err(query.commit_outcome_error(error.to_string()));
            }
            session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
        } else if fenced {
            query.exit_commit_critical()?;
        }
        let response: Result<Vec<RecordBatch>> = (|| {
            let json = serde_json::to_string(&procedure_output_json(&result.output))
                .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
            Ok(vec![json_batch("result_json", vec![json])?])
        })();
        session.clear_cache();
        return response.map(Some).map_err(|error| {
            if result.epoch.is_some() {
                query.commit_outcome_error(error.to_string())
            } else {
                error
            }
        });
    }

    if let Some(table) = lower
        .strip_prefix("describe ")
        .or_else(|| lower.strip_prefix("desc "))
    {
        let table = strip_identifier(table.trim())?;
        return Ok(Some(vec![describe_table(db, table)?]));
    }

    if lower.starts_with("pragma ") {
        return Ok(Some(vec![run_pragma(session, db, sql, lower, query)?]));
    }

    if lower == "check" || lower == "check database" {
        return Ok(Some(vec![check_batch(db, query)?]));
    }

    if lower == "doctor" || lower == "doctor database" {
        let mut fence_error = None;
        let mut fenced = false;
        let quarantined = db.doctor_controlled_with_receipt(query.control(), || {
            match enter_commit_fence(session, query) {
                Ok(()) => {
                    fenced = true;
                    true
                }
                Err(error) => {
                    fence_error = Some(error);
                    false
                }
            }
        });
        if let Some(error) = fence_error {
            return Err(error);
        }
        let (quarantined, receipt) = match quarantined {
            Ok(result) => result,
            Err(error) => {
                if fenced {
                    return Err(uncertain_fenced_error(session, query, error));
                } else {
                    query.checkpoint()?;
                }
                return Err(error.into());
            }
        };
        if fenced {
            let Some(receipt) = receipt else {
                return Err(uncertain_fenced_error(
                    session,
                    query,
                    mongreldb_core::MongrelError::Other(
                        "DOCTOR published without a maintenance receipt".into(),
                    ),
                ));
            };
            query.record_commit(query.status().statement_index, receipt.epoch.0);
            if let Err(error) = query.exit_commit_critical() {
                return Err(query.commit_outcome_error(error.to_string()));
            }
            session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
        }
        let values: Vec<String> = quarantined.into_iter().map(|id| id.to_string()).collect();
        session.clear_cache();
        return Ok(Some(vec![strings_batch("quarantined_table_id", values)?]));
    }

    if lower.starts_with("vacuum into ") {
        let target = parse_vacuum_into(sql, lower)?;
        let mut fence_error = None;
        let mut fenced = false;
        let report =
            db.hot_backup_controlled(
                Path::new(target),
                query.control(),
                || match enter_commit_fence(session, query) {
                    Ok(()) => {
                        fenced = true;
                        true
                    }
                    Err(error) => {
                        fence_error = Some(error);
                        false
                    }
                },
            );
        if let Some(error) = fence_error {
            return Err(error);
        }
        let report = match report {
            Ok(report) => report,
            Err(error) => {
                if fenced {
                    return Err(uncertain_fenced_error(session, query, error));
                } else {
                    query.checkpoint()?;
                }
                return Err(error.into());
            }
        };
        query.record_commit(query.status().statement_index, report.epoch);
        if let Err(error) = query.exit_commit_critical() {
            return Err(query.commit_outcome_error(error.to_string()));
        }
        session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if lower == "compact" || lower == "compact database" || lower == "vacuum" {
        compact_all(session, db, query)?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if lower == "analyze" || lower.starts_with("analyze ") {
        analyze_all(session, db, query)?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if lower == "reindex" || lower.starts_with("reindex ") {
        let target = lower
            .strip_prefix("reindex")
            .map(str::trim)
            .unwrap_or_default();
        let target = if target.is_empty() {
            None
        } else {
            Some(strip_identifier(target)?)
        };
        reindex(session, db, target, query)?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if sql.ends_with(';') {
        return try_manual_command(
            session,
            sql.trim_end_matches(';').trim(),
            lower.trim_end_matches(';'),
            query,
        );
    }

    Ok(None)
}

fn parse_grant_permission(
    db: &Arc<Database>,
    permission: &str,
    table: &str,
) -> Result<mongreldb_core::Permission> {
    use mongreldb_core::Permission;

    let lower = permission.to_ascii_lowercase();
    if let Some(open) = permission.find('(') {
        let close = permission.rfind(')').ok_or_else(|| {
            MongrelQueryError::Schema("column permission requires closing ')'".into())
        })?;
        if close < open || !permission[close + 1..].trim().is_empty() {
            return Err(MongrelQueryError::Schema(
                "invalid column permission list".into(),
            ));
        }
        let operation = lower[..open].trim();
        let schema = table_schema(db, table)?;
        let mut columns = permission[open + 1..close]
            .split(',')
            .map(|column| column.trim().trim_matches('"').to_string())
            .collect::<Vec<_>>();
        if columns.is_empty() || columns.iter().any(String::is_empty) {
            return Err(MongrelQueryError::Schema(
                "column permission list cannot be empty".into(),
            ));
        }
        for column in &columns {
            if schema.column(column).is_none() {
                return Err(MongrelQueryError::Schema(format!(
                    "unknown grant column {column} on {table}"
                )));
            }
        }
        columns.sort();
        columns.dedup();
        return match operation {
            "select" => Ok(Permission::SelectColumns {
                table: table.to_string(),
                columns,
            }),
            "insert" => Ok(Permission::InsertColumns {
                table: table.to_string(),
                columns,
            }),
            "update" => Ok(Permission::UpdateColumns {
                table: table.to_string(),
                columns,
            }),
            _ => Err(MongrelQueryError::Schema(format!(
                "column grants are unsupported for {operation}"
            ))),
        };
    }
    match lower.trim() {
        "select" => Ok(Permission::Select {
            table: table.to_string(),
        }),
        "insert" => Ok(Permission::Insert {
            table: table.to_string(),
        }),
        "update" => Ok(Permission::Update {
            table: table.to_string(),
        }),
        "delete" => Ok(Permission::Delete {
            table: table.to_string(),
        }),
        "all" => Ok(Permission::All),
        "ddl" => Ok(Permission::Ddl),
        "admin" => Ok(Permission::Admin),
        other => Err(MongrelQueryError::Schema(format!(
            "unknown permission {other}"
        ))),
    }
}

fn create_mask(session: &MongrelSession, sql: &str, query: &RegisteredSqlQuery) -> Result<()> {
    let Some(db) = &session.database else {
        return Ok(());
    };
    let body = sql["create mask ".len()..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let lower = body.to_ascii_lowercase();
    let on = lower
        .find(" on ")
        .ok_or_else(|| MongrelQueryError::Schema("CREATE MASK requires ON".into()))?;
    let name = body[..on].trim().trim_matches('"').to_string();
    if name.is_empty() {
        return Err(MongrelQueryError::Schema(
            "CREATE MASK requires a name".into(),
        ));
    }
    let after_on = &body[on + " on ".len()..];
    let after_on_lower = after_on.to_ascii_lowercase();
    let using = after_on_lower
        .find(" using ")
        .ok_or_else(|| MongrelQueryError::Schema("CREATE MASK requires USING".into()))?;
    let target = after_on[..using].trim();
    let open = target.find('(').ok_or_else(|| {
        MongrelQueryError::Schema("CREATE MASK target must be table(column)".into())
    })?;
    let close = target.rfind(')').ok_or_else(|| {
        MongrelQueryError::Schema("CREATE MASK target must be table(column)".into())
    })?;
    if close < open || !target[close + 1..].trim().is_empty() {
        return Err(MongrelQueryError::Schema(
            "CREATE MASK target must be table(column)".into(),
        ));
    }
    let table = target[..open].trim().trim_matches('"').to_string();
    let column_name = target[open + 1..close].trim().trim_matches('"').to_string();
    let schema = table_schema(db, &table)?;
    let column = schema
        .column(&column_name)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown mask column {column_name}")))?;
    let strategy_and_except = after_on[using + " using ".len()..].trim();
    let lower_strategy = strategy_and_except.to_ascii_lowercase();
    let except = lower_strategy.find(" except ");
    let strategy_text = except
        .map(|index| strategy_and_except[..index].trim())
        .unwrap_or(strategy_and_except);
    let exempt_subjects = except
        .map(|index| parse_mask_subjects(&strategy_and_except[index + " except ".len()..]))
        .transpose()?
        .unwrap_or_default();
    let lower_strategy = strategy_text.to_ascii_lowercase();
    let strategy = if lower_strategy == "null" {
        mongreldb_core::MaskStrategy::Null
    } else if lower_strategy == "sha256" {
        mongreldb_core::MaskStrategy::Sha256
    } else if lower_strategy.starts_with("redact ") {
        mongreldb_core::MaskStrategy::Redact {
            replacement: unquote_sql_string(strategy_text["redact ".len()..].trim())?.to_string(),
        }
    } else {
        return Err(MongrelQueryError::Schema(
            "mask strategy must be NULL, SHA256, or REDACT '<text>'".into(),
        ));
    };
    let mut security = db.security_catalog();
    if security
        .masks
        .iter()
        .any(|mask| mask.table == table && mask.name == name)
    {
        return Err(MongrelQueryError::Schema(format!(
            "mask {name} already exists on {table}"
        )));
    }
    security.masks.push(mongreldb_core::ColumnMask {
        name,
        table: table.clone(),
        column: column.id,
        strategy,
        exempt_subjects,
    });
    run_controlled_durable_with_epoch(session, query, |before_publish| {
        let epoch = db.set_security_catalog_as_with_epoch_controlled(
            security,
            session.principal().as_ref(),
            before_publish,
        )?;
        Ok(((), epoch.0))
    })?;
    post_commit_result(query, session.refresh_registered_table(db, &table))?;
    session.clear_cache();
    Ok(())
}

fn parse_mask_subjects(input: &str) -> Result<Vec<String>> {
    let input = input.trim();
    if !input.starts_with('(') || !input.ends_with(')') {
        return Err(MongrelQueryError::Schema(
            "mask EXCEPT requires a parenthesized subject list".into(),
        ));
    }
    let subjects = input[1..input.len() - 1]
        .split(',')
        .map(|subject| subject.trim().trim_matches('"').to_string())
        .collect::<Vec<_>>();
    if subjects.is_empty() || subjects.iter().any(String::is_empty) {
        return Err(MongrelQueryError::Schema(
            "mask EXCEPT subject list cannot be empty".into(),
        ));
    }
    Ok(subjects)
}

fn drop_mask(session: &MongrelSession, sql: &str, query: &RegisteredSqlQuery) -> Result<()> {
    let Some(db) = &session.database else {
        return Ok(());
    };
    let mut body = sql["drop mask ".len()..]
        .trim()
        .trim_end_matches(';')
        .trim();
    let if_exists = body.to_ascii_lowercase().starts_with("if exists ");
    if if_exists {
        body = body["if exists ".len()..].trim();
    }
    let lower = body.to_ascii_lowercase();
    let on = lower
        .find(" on ")
        .ok_or_else(|| MongrelQueryError::Schema("DROP MASK requires ON <table>".into()))?;
    let name = body[..on].trim().trim_matches('"');
    let table = body[on + " on ".len()..].trim().trim_matches('"');
    let mut security = db.security_catalog();
    let old_len = security.masks.len();
    security
        .masks
        .retain(|mask| mask.table != table || mask.name != name);
    if security.masks.len() == old_len {
        if if_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "mask {name} does not exist on {table}"
        )));
    }
    run_controlled_durable_with_epoch(session, query, |before_publish| {
        let epoch = db.set_security_catalog_as_with_epoch_controlled(
            security,
            session.principal().as_ref(),
            before_publish,
        )?;
        Ok(((), epoch.0))
    })?;
    post_commit_result(query, session.refresh_registered_table(db, table))?;
    session.clear_cache();
    Ok(())
}

/// `ATTACH 'path' AS alias` — open a second MongrelDB database directory and
/// register all its tables on the session's DataFusion context, prefixed with
/// `alias.`. This enables cross-database SQL joins (e.g.
/// `SELECT * FROM alias.users JOIN local.orders`).
fn attach_database(session: &MongrelSession, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    // Parse: ATTACH 'path' AS alias  (also: ATTACH DATABASE 'path' AS alias)
    let lower = sql.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("attach ")
        .unwrap_or("")
        .strip_prefix("database ")
        .unwrap_or("");
    // Extract quoted path and AS alias.
    let (path, alias) = parse_attach_args(rest, sql)?;
    let attached_db = Arc::new(Database::open(&path)?);
    let table_names = attached_db.table_names();
    for name in &table_names {
        let handle = attached_db.table(name)?;
        let provider = MongrelProvider::new_handle(handle.clone())?;
        // Register under a qualified name `{alias}_{name}` (using underscore,
        // not dot — DataFusion's `schema.table` resolution requires catalog
        // setup for dot-qualified names). This avoids collisions with tables
        // from the primary database.
        let qualified = format!("{alias}_{name}");
        session
            .ctx
            .register_table(&qualified, Arc::new(provider))
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        session.tables.lock().insert(qualified, handle);
        // Also register the bare name if no collision exists.
        let bare_provider = MongrelProvider::new_handle(attached_db.table(name)?)?;
        if session
            .ctx
            .register_table(name, Arc::new(bare_provider))
            .is_ok()
        {
            session
                .tables
                .lock()
                .insert(name.clone(), attached_db.table(name)?);
        }
    }
    // Store the attached Database so it stays alive for the session's lifetime.
    session
        .attached_databases
        .lock()
        .insert(alias.clone(), attached_db);
    session.clear_cache();
    Ok(Some(Vec::new()))
}

/// `DETACH alias` — unregister all tables from the attached database.
fn detach_database(session: &MongrelSession, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    let lower = sql.to_ascii_lowercase();
    let rest = lower.strip_prefix("detach ").unwrap_or("");
    let alias = parse_detach_alias(rest)?;
    let removed = session.attached_databases.lock().remove(&alias);
    if removed.is_none() {
        // DETACH of a non-attached alias is a no-op (SQLite: error, but we
        // tolerate it for idempotency).
        return Ok(Some(Vec::new()));
    }
    // Remove qualified and bare table registrations belonging to this alias.
    let mut tables = session.tables.lock();
    let to_remove: Vec<String> = tables
        .keys()
        .filter(|k| k.starts_with(&format!("{alias}_")))
        .cloned()
        .collect();
    for name in &to_remove {
        tables.remove(name);
        let _ = session.ctx.deregister_table(name);
    }
    session.clear_cache();
    Ok(Some(Vec::new()))
}

/// Extract the path and alias from `ATTACH ... 'path' AS alias` arguments.
fn parse_attach_args(_lower_rest: &str, original_sql: &str) -> Result<(PathBuf, String)> {
    // Parse entirely from the original SQL (not the lowercased version) to avoid
    // any case/whitespace surprises. Pattern: ATTACH [DATABASE] 'path' AS alias
    let sql_lower = original_sql.to_ascii_lowercase();
    // Find the single-quoted path.
    let path_start = original_sql.find('\'').ok_or_else(|| {
        MongrelQueryError::Schema("ATTACH requires a quoted path: ATTACH 'path' AS alias".into())
    })?;
    let path_end = original_sql[path_start + 1..]
        .find('\'')
        .ok_or_else(|| MongrelQueryError::Schema("ATTACH path missing closing quote".into()))?;
    let path = PathBuf::from(&original_sql[path_start + 1..path_start + 1 + path_end]);
    // Find the alias: everything after the closing quote, split on " as "
    // (case-insensitive). The alias is the last token, trimmed of semicolons.
    let after_path = &sql_lower[path_start + 1 + path_end + 1..];
    let alias_part = if let Some(as_pos) = after_path.find(" as ") {
        &after_path[as_pos + 4..]
    } else {
        // Also accept the SQL keyword without spaces around it.
        after_path.trim_start()
    };
    let alias = alias_part.trim().trim_end_matches(';').trim().to_string();
    if alias.is_empty() {
        return Err(MongrelQueryError::Schema(
            "ATTACH requires AS alias: ATTACH 'path' AS alias".into(),
        ));
    }
    Ok((path, alias))
}

fn parse_notify_args(_lower_rest: &str, original_rest: &str) -> Result<(String, Option<String>)> {
    // Pattern: NOTIFY channel_name  or  NOTIFY channel_name, 'payload'
    let trimmed = original_rest.trim().trim_end_matches(';').trim();
    if let Some((channel, payload)) = trimmed.split_once(',') {
        let channel = channel
            .trim()
            .trim_matches(|c| c == '"' || c == '\'')
            .to_string();
        let payload = payload.trim().trim_matches(|c| c == '\'').to_string();
        Ok((channel, Some(payload)))
    } else {
        let channel = trimmed.trim_matches(|c| c == '"' || c == '\'').to_string();
        if channel.is_empty() {
            return Err(MongrelQueryError::Schema(
                "NOTIFY requires a channel name".into(),
            ));
        }
        Ok((channel, None))
    }
}

fn parse_detach_alias(lower_rest: &str) -> Result<String> {
    let alias = lower_rest
        .strip_prefix("database ")
        .unwrap_or(lower_rest)
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();
    if alias.is_empty() {
        return Err(MongrelQueryError::Schema("DETACH requires an alias".into()));
    }
    Ok(alias)
}

async fn create_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    create: &CreateTable,
    ttl: Option<&ParsedTtlClause>,
    registered_query: &RegisteredSqlQuery,
) -> Result<()> {
    let name = object_name(&create.name)?;
    if create.if_not_exists && db.table_id(&name).is_ok() {
        return Ok(());
    }

    // CREATE TABLE AS SELECT — execute the query, infer the schema from the
    // result, create the table, and bulk-insert the rows.
    if let Some(source_query) = create.query.as_deref() {
        if ttl.is_some() {
            return Err(MongrelQueryError::Schema(
                "CREATE TABLE AS SELECT does not support TTL_COLUMN".into(),
            ));
        }
        return create_table_as_select(session, db, &name, source_query, registered_query, false)
            .await;
    }

    let schema = schema_from_create_table(create)?;
    if let Some(ttl) = ttl {
        let column = schema
            .columns
            .iter()
            .find(|column| column.name == ttl.column)
            .ok_or_else(|| {
                MongrelQueryError::Schema(format!("unknown TTL column {}", ttl.column))
            })?;
        if column.ty != TypeId::TimestampNanos {
            return Err(MongrelQueryError::Schema(format!(
                "TTL column {} must be TIMESTAMP",
                ttl.column
            )));
        }
    }
    let temp_table = format!("__mongreldb_ctas_build_{}", registered_query.id());
    db.create_building_table(
        &temp_table,
        &name,
        &registered_query.id().to_string(),
        schema,
    )?;
    if let Some(ttl) = ttl {
        if let Err(error) = db.set_building_table_ttl(&temp_table, &ttl.column, ttl.duration_nanos)
        {
            let _ = db.discard_building_table(&temp_table);
            return Err(error.into());
        }
    }
    let publish_epoch =
        match run_controlled_durable_with_epoch(session, registered_query, |before_commit| {
            let epoch = db.publish_building_table_controlled(&temp_table, &name, before_commit)?;
            Ok((epoch, epoch.0))
        }) {
            Ok(epoch) => epoch,
            Err(error) => {
                let may_be_published = matches!(
                    &error,
                    MongrelQueryError::CommitOutcome {
                        committed: true,
                        ..
                    } | MongrelQueryError::OutcomeUnknown { .. }
                );
                if may_be_published && db.table_id(&name).is_ok() {
                    if let Err(register_error) = register_table(session, db, &name) {
                        let message =
                            format!("{error}; table registration failed: {register_error}");
                        return Err(
                            if matches!(&error, MongrelQueryError::OutcomeUnknown { .. }) {
                                registered_query.outcome_unknown_error(message)
                            } else {
                                registered_query.commit_outcome_error(message)
                            },
                        );
                    }
                    session.clear_cache();
                    return Err(error);
                }
                let _ = db.discard_building_table(&temp_table);
                return Err(error);
            }
        };
    let _ = publish_epoch;
    if let Err(error) = register_table(session, db, &name) {
        return Err(registered_query.commit_outcome_error(error.to_string()));
    }
    session.clear_cache();
    Ok(())
}

/// Execute `CREATE TABLE name AS SELECT ...`: run the inner query, infer the
/// table schema from the Arrow result, create the table, and insert all rows.
fn create_table_as_select<'a>(
    session: &'a MongrelSession,
    db: &'a Arc<Database>,
    table_name: &'a str,
    source_query: &'a sqlparser::ast::Query,
    query: &'a RegisteredSqlQuery,
    materialized: bool,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
    let select_sql = format!("SELECT * FROM ({source_query}) AS ctas_source");
    Box::pin(async move {
        query.checkpoint()?;
        let mut stream = session
            .execute_command_source_stream(&select_sql, query)
            .await?;
        let arrow_schema = stream.schema();

        query.checkpoint()?;
        use mongreldb_core::schema::{
            ColumnDef as CoreColumnDef, ColumnFlags, Schema as CoreSchema,
        };
        if arrow_schema.fields().is_empty() {
            return Err(MongrelQueryError::Schema(
                "CREATE TABLE AS SELECT requires at least one result column".into(),
            ));
        }
        let mut columns = Vec::new();
        for (i, field) in arrow_schema.fields().iter().enumerate() {
            let ty = arrow_data_type_to_type_id(field.data_type())?;
            columns.push(CoreColumnDef {
                id: (i + 1) as u16,
                name: field.name().clone(),
                ty,
                flags: if i == 0 {
                    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)
                } else {
                    ColumnFlags::empty().with(ColumnFlags::NULLABLE)
                },
                default_value: None,
                embedding_source: None,
            });
        }

        let schema = CoreSchema {
            schema_id: 0,
            columns,
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        let target_schema = schema.clone();

        // Stream into a durable hidden table one batch at a time. Hidden batch
        // commits are reclaimable implementation state, not user-visible SQL
        // commits. Only the final catalog publish is fenced and recorded.
        let temp_table = format!("__mongreldb_ctas_build_{}", query.id());
        db.create_building_table(&temp_table, table_name, &query.id().to_string(), schema)?;
        let mut saw_batch = false;
        let mut converted = 0_usize;
        loop {
            let batch = match next_command_batch(&mut stream, query).await {
                Ok(Some(batch)) => batch,
                Ok(None) => break,
                Err(error) => {
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error);
                }
            };
            saw_batch = true;
            let batch_bytes = batch
                .columns()
                .iter()
                .map(|column| column.get_array_memory_size())
                .fold(0_usize, usize::saturating_add);
            if batch_bytes > CTAS_INPUT_BATCH_BYTES_LIMIT {
                let _ = db.discard_building_table(&temp_table);
                return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "CREATE TABLE AS SELECT input batch bytes",
                    requested: batch_bytes,
                    limit: CTAS_INPUT_BATCH_BYTES_LIMIT,
                }
                .into());
            }
            let mut row_idx = 0_usize;
            while row_idx < batch.num_rows() {
                let mut txn = db.begin_as(session.principal());
                let mut staged_rows = 0_usize;
                let mut staged_bytes = 0_usize;
                while row_idx < batch.num_rows() && staged_rows < CTAS_STAGING_ROWS_LIMIT {
                    if let Err(error) = command_checkpoint(session, query, converted) {
                        drop(txn);
                        let _ = db.discard_building_table(&temp_table);
                        return Err(error);
                    }
                    let mut cells = Vec::with_capacity(batch.num_columns());
                    for (col_idx, col) in batch.columns().iter().enumerate() {
                        let value = match arrow_cell_to_value(col, row_idx) {
                            Ok(value) => value,
                            Err(error) => {
                                drop(txn);
                                let _ = db.discard_building_table(&temp_table);
                                return Err(error);
                            }
                        };
                        cells.push(((col_idx + 1) as u16, value));
                    }
                    if matches!(cells.first(), Some((_, Value::Null))) {
                        drop(txn);
                        let _ = db.discard_building_table(&temp_table);
                        return Err(MongrelQueryError::Schema(
                            "CREATE TABLE AS SELECT inferred a NULL primary key".into(),
                        ));
                    }
                    let row_bytes = cells_deep_bytes(&cells);
                    if row_bytes > CTAS_STAGING_BYTES_LIMIT {
                        drop(txn);
                        let _ = db.discard_building_table(&temp_table);
                        return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                            resource: "CREATE TABLE AS SELECT staged row bytes",
                            requested: row_bytes,
                            limit: CTAS_STAGING_BYTES_LIMIT,
                        }
                        .into());
                    }
                    if staged_rows > 0
                        && staged_bytes.saturating_add(row_bytes) > CTAS_STAGING_BYTES_LIMIT
                    {
                        break;
                    }
                    if let Err(error) = txn.put_building(&temp_table, cells) {
                        drop(txn);
                        let _ = db.discard_building_table(&temp_table);
                        return Err(error.into());
                    }
                    staged_rows += 1;
                    staged_bytes = staged_bytes.saturating_add(row_bytes);
                    row_idx += 1;
                    converted += 1;
                }
                if staged_rows > 0 {
                    if let Err(error) = txn.commit_controlled(query.control(), || Ok(())) {
                        let _ = db.discard_building_table(&temp_table);
                        return Err(error.into());
                    }
                }
            }
        }
        if !saw_batch {
            let _ = db.discard_building_table(&temp_table);
            return Err(MongrelQueryError::Schema(
                "CREATE TABLE AS SELECT produced no result (cannot infer schema)".into(),
            ));
        }
        let materialized_definition = if materialized {
            let mut incremental = match infer_incremental_aggregate_with_schema(
                db,
                table_name,
                source_query,
                &target_schema,
            ) {
                Ok(incremental) => incremental,
                Err(error) => {
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error);
                }
            };
            if let Some(plan) = incremental.as_mut() {
                let (groups, snapshot) =
                    match collect_incremental_aggregate_groups(session, db, plan, query) {
                        Ok(result) => result,
                        Err(error) => {
                            let _ = db.discard_building_table(&temp_table);
                            return Err(error);
                        }
                    };
                plan.checkpoint_event_id = format!("{}:{}", snapshot.epoch.0, u32::MAX);
                let mut transaction = db.begin_as(session.principal());
                if let Err(error) = transaction.truncate_building(&temp_table) {
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error.into());
                }
                if let Err(error) = transaction.commit_controlled(query.control(), || Ok(())) {
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error.into());
                }
                let mut chunk = Vec::new();
                let mut chunk_bytes = 0_usize;
                for (index, state) in groups.into_values().enumerate() {
                    if let Err(error) = command_checkpoint(session, query, index) {
                        let _ = db.discard_building_table(&temp_table);
                        return Err(error);
                    }
                    let cells = match aggregate_cells(plan, state.group, state.count, &state.sums) {
                        Ok(cells) => cells,
                        Err(error) => {
                            let _ = db.discard_building_table(&temp_table);
                            return Err(error);
                        }
                    };
                    let row_bytes = cells_deep_bytes(&cells);
                    if row_bytes > CTAS_STAGING_BYTES_LIMIT {
                        let _ = db.discard_building_table(&temp_table);
                        return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                            resource: "incremental materialized-view staged row bytes",
                            requested: row_bytes,
                            limit: CTAS_STAGING_BYTES_LIMIT,
                        }
                        .into());
                    }
                    if !chunk.is_empty()
                        && (chunk.len() >= CTAS_STAGING_ROWS_LIMIT
                            || chunk_bytes.saturating_add(row_bytes) > CTAS_STAGING_BYTES_LIMIT)
                    {
                        if let Err(error) = commit_rebuild_chunk(
                            session,
                            db,
                            &temp_table,
                            std::mem::take(&mut chunk),
                            query,
                        ) {
                            let _ = db.discard_building_table(&temp_table);
                            return Err(error);
                        }
                        chunk_bytes = 0;
                    }
                    chunk_bytes = chunk_bytes.saturating_add(row_bytes);
                    chunk.push(cells);
                }
                if let Err(error) = commit_rebuild_chunk(session, db, &temp_table, chunk, query) {
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error);
                }
            }
            Some(mongreldb_core::MaterializedViewEntry {
                name: table_name.to_string(),
                query: source_query.to_string(),
                last_refresh_epoch: 0,
                incremental,
            })
        } else {
            None
        };
        let publish_epoch =
            match run_controlled_durable_with_epoch(session, query, |before_commit| {
                let epoch = if let Some(definition) = materialized_definition {
                    db.publish_materialized_building_table_controlled(
                        &temp_table,
                        table_name,
                        definition,
                        before_commit,
                    )?
                } else {
                    db.publish_building_table_controlled(&temp_table, table_name, before_commit)?
                };
                Ok((epoch, epoch.0))
            }) {
                Ok(epoch) => epoch,
                Err(error) => {
                    let may_be_published = matches!(
                        &error,
                        MongrelQueryError::CommitOutcome {
                            committed: true,
                            ..
                        } | MongrelQueryError::OutcomeUnknown { .. }
                    );
                    if may_be_published && db.table_id(table_name).is_ok() {
                        if let Err(register_error) = register_table(session, db, table_name) {
                            let message =
                                format!("{error}; table registration failed: {register_error}");
                            return Err(
                                if matches!(&error, MongrelQueryError::OutcomeUnknown { .. }) {
                                    query.outcome_unknown_error(message)
                                } else {
                                    query.commit_outcome_error(message)
                                },
                            );
                        }
                        session.clear_cache();
                        return Err(error);
                    }
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error);
                }
            };
        let _ = publish_epoch;
        if let Err(error) = register_table(session, db, table_name) {
            return Err(query.commit_outcome_error(error.to_string()));
        }
        session.clear_cache();
        Ok(())
    })
}

/// Map an Arrow `DataType` to a MongrelDB `TypeId`.
fn arrow_data_type_to_type_id(dt: &arrow::datatypes::DataType) -> Result<TypeId> {
    use arrow::datatypes::DataType;
    Ok(match dt {
        DataType::Boolean => TypeId::Bool,
        DataType::Int8 => TypeId::Int8,
        DataType::Int16 => TypeId::Int16,
        DataType::Int32 => TypeId::Int32,
        DataType::Int64 => TypeId::Int64,
        DataType::UInt8 => TypeId::UInt8,
        DataType::UInt16 => TypeId::UInt16,
        DataType::UInt32 => TypeId::UInt32,
        DataType::UInt64 => TypeId::UInt64,
        DataType::Float32 => TypeId::Float32,
        DataType::Float64 => TypeId::Float64,
        DataType::Utf8 | DataType::LargeUtf8 => TypeId::Bytes,
        DataType::Binary | DataType::LargeBinary => TypeId::Bytes,
        DataType::Date32 => TypeId::Date32,
        DataType::Date64 => TypeId::Date64,
        DataType::Timestamp(_, _) => TypeId::TimestampNanos,
        _ => {
            return Err(MongrelQueryError::Schema(format!(
                "CREATE TABLE AS SELECT does not support Arrow type {dt:?}"
            )))
        }
    })
}

/// Extract a MongrelDB `Value` from an Arrow array cell at `row_idx`.
fn typed_arrow_array<'a, T: 'static>(
    array: &'a std::sync::Arc<dyn arrow::array::Array>,
    expected: &str,
) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| MongrelQueryError::Arrow(format!("expected {expected}")))
}

fn arrow_cell_to_value(
    array: &std::sync::Arc<dyn arrow::array::Array>,
    row_idx: usize,
) -> Result<Value> {
    use arrow::array::*;
    if row_idx >= array.len() {
        return Err(MongrelQueryError::Arrow(format!(
            "Arrow row index {row_idx} exceeds array length {}",
            array.len()
        )));
    }
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }
    Ok(match array.data_type() {
        arrow::datatypes::DataType::Boolean => {
            Value::Bool(typed_arrow_array::<BooleanArray>(array, "BooleanArray")?.value(row_idx))
        }
        arrow::datatypes::DataType::Int8 => {
            Value::Int64(typed_arrow_array::<Int8Array>(array, "Int8Array")?.value(row_idx) as i64)
        }
        arrow::datatypes::DataType::Int16 => Value::Int64(
            typed_arrow_array::<Int16Array>(array, "Int16Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::Int32 => Value::Int64(
            typed_arrow_array::<Int32Array>(array, "Int32Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::Int64 => {
            Value::Int64(typed_arrow_array::<Int64Array>(array, "Int64Array")?.value(row_idx))
        }
        arrow::datatypes::DataType::UInt8 => Value::Int64(
            typed_arrow_array::<UInt8Array>(array, "UInt8Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::UInt16 => Value::Int64(
            typed_arrow_array::<UInt16Array>(array, "UInt16Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::UInt32 => Value::Int64(
            typed_arrow_array::<UInt32Array>(array, "UInt32Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::UInt64 => Value::Int64(
            typed_arrow_array::<UInt64Array>(array, "UInt64Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::Float32 => Value::Float64(
            typed_arrow_array::<Float32Array>(array, "Float32Array")?.value(row_idx) as f64,
        ),
        arrow::datatypes::DataType::Float64 => {
            Value::Float64(typed_arrow_array::<Float64Array>(array, "Float64Array")?.value(row_idx))
        }
        arrow::datatypes::DataType::Utf8 => Value::Bytes(
            typed_arrow_array::<StringArray>(array, "StringArray")?
                .value(row_idx)
                .as_bytes()
                .to_vec(),
        ),
        arrow::datatypes::DataType::LargeUtf8 => Value::Bytes(
            typed_arrow_array::<LargeStringArray>(array, "LargeStringArray")?
                .value(row_idx)
                .as_bytes()
                .to_vec(),
        ),
        arrow::datatypes::DataType::Binary => Value::Bytes(
            typed_arrow_array::<BinaryArray>(array, "BinaryArray")?
                .value(row_idx)
                .to_vec(),
        ),
        arrow::datatypes::DataType::LargeBinary => Value::Bytes(
            typed_arrow_array::<LargeBinaryArray>(array, "LargeBinaryArray")?
                .value(row_idx)
                .to_vec(),
        ),
        arrow::datatypes::DataType::Date32 => Value::Int64(
            typed_arrow_array::<Date32Array>(array, "Date32Array")?.value(row_idx) as i64,
        ),
        arrow::datatypes::DataType::Date64 => {
            Value::Int64(typed_arrow_array::<Date64Array>(array, "Date64Array")?.value(row_idx))
        }
        arrow::datatypes::DataType::Timestamp(unit, _) => {
            let (value, multiplier) = match unit {
                arrow::datatypes::TimeUnit::Second => (
                    typed_arrow_array::<TimestampSecondArray>(array, "TimestampSecondArray")?
                        .value(row_idx),
                    1_000_000_000,
                ),
                arrow::datatypes::TimeUnit::Millisecond => (
                    typed_arrow_array::<TimestampMillisecondArray>(
                        array,
                        "TimestampMillisecondArray",
                    )?
                    .value(row_idx),
                    1_000_000,
                ),
                arrow::datatypes::TimeUnit::Microsecond => (
                    typed_arrow_array::<TimestampMicrosecondArray>(
                        array,
                        "TimestampMicrosecondArray",
                    )?
                    .value(row_idx),
                    1_000,
                ),
                arrow::datatypes::TimeUnit::Nanosecond => (
                    typed_arrow_array::<TimestampNanosecondArray>(
                        array,
                        "TimestampNanosecondArray",
                    )?
                    .value(row_idx),
                    1,
                ),
            };
            Value::Int64(value.checked_mul(multiplier).ok_or_else(|| {
                MongrelQueryError::Arrow("timestamp overflows nanosecond storage".into())
            })?)
        }
        other => {
            return Err(MongrelQueryError::Schema(format!(
                "CTAS does not support value extraction from Arrow type {other:?}"
            )))
        }
    })
}

fn create_virtual_table_manual(
    session: &MongrelSession,
    db: &Arc<Database>,
    sql: &str,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let lower = sql.to_ascii_lowercase();
    let mut rest = lower
        .strip_prefix("create virtual table ")
        .ok_or_else(|| MongrelQueryError::Schema("expected CREATE VIRTUAL TABLE".into()))?;
    let mut if_not_exists = false;
    let original_rest_start = "create virtual table ".len();
    let mut original_rest = &sql[original_rest_start..];
    if rest.starts_with("if not exists ") {
        if_not_exists = true;
        rest = &rest["if not exists ".len()..];
        original_rest = &original_rest["if not exists ".len()..];
    }
    let Some(using_pos) = rest.find(" using ") else {
        return Err(MongrelQueryError::Schema(
            "CREATE VIRTUAL TABLE requires USING module".into(),
        ));
    };
    let name = strip_identifier(original_rest[..using_pos].trim())?.to_string();
    let after_using = original_rest[using_pos + " using ".len()..].trim();
    let (module, args) = if let Some(open) = after_using.find('(') {
        let close = after_using
            .rfind(')')
            .ok_or_else(|| MongrelQueryError::Schema("CREATE VIRTUAL TABLE missing ')'".into()))?;
        let module = strip_identifier(after_using[..open].trim())?.to_string();
        let args = split_module_args(&after_using[open + 1..close])?;
        (module, args)
    } else {
        (strip_identifier(after_using)?.to_string(), Vec::new())
    };
    create_virtual_table_named(session, db, name, if_not_exists, module, args, query)
}

fn split_module_args(raw: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut expected_closers = Vec::new();
    let mut chars = raw.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(active_quote) = quote {
            if ch == active_quote {
                if chars.peek().is_some_and(|(_, next)| *next == active_quote) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '(' => expected_closers.push(')'),
            '[' => expected_closers.push(']'),
            '{' => expected_closers.push('}'),
            ')' | ']' | '}' => {
                if expected_closers.pop() != Some(ch) {
                    return Err(MongrelQueryError::Schema(
                        "unbalanced CREATE VIRTUAL TABLE module argument delimiters".into(),
                    ));
                }
            }
            ',' if expected_closers.is_empty() => {
                push_module_arg(&mut args, &raw[start..idx])?;
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    if quote.is_some() {
        return Err(MongrelQueryError::Schema(
            "unterminated CREATE VIRTUAL TABLE module argument string".into(),
        ));
    }
    if !expected_closers.is_empty() {
        return Err(MongrelQueryError::Schema(
            "unbalanced CREATE VIRTUAL TABLE module argument delimiters".into(),
        ));
    }
    push_module_arg(&mut args, &raw[start..])?;
    Ok(args)
}

fn push_module_arg(args: &mut Vec<String>, raw: &str) -> Result<()> {
    let raw = raw.trim();
    if !raw.is_empty() {
        args.push(strip_identifier(raw)?.to_string());
    }
    Ok(())
}

fn create_virtual_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: ObjectName,
    if_not_exists: bool,
    module_name: String,
    module_args: Vec<String>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let name = object_name(&name)?;
    create_virtual_table_named(
        session,
        db,
        name,
        if_not_exists,
        module_name,
        module_args,
        query,
    )
}

fn create_virtual_table_named(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: String,
    if_not_exists: bool,
    module_name: String,
    module_args: Vec<String>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if db.table_id(&name).is_ok() || db.external_table(&name).is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "table {name:?} already exists"
        )));
    }
    let module = module_name.to_ascii_lowercase();
    let descriptor = session.external_modules.descriptor(&module)?;
    let args = module_args
        .into_iter()
        .map(ModuleArg::Ident)
        .collect::<Vec<_>>();
    let entry = ExternalTableEntry::new(
        name.clone(),
        ExternalTableDefinition {
            module,
            args,
            declared_schema: descriptor.schema,
            hidden_columns: descriptor.hidden_columns,
            options: Default::default(),
            capabilities: descriptor.capabilities,
        },
        0,
    )
    .map_err(MongrelQueryError::Core)?;
    // Validate module arguments and construct the provider before the durable
    // catalog write. A rejected module definition must not survive a reopen.
    let provider = session
        .external_modules
        .external_table_provider(db, &entry, Some(query))?;
    let entry = run_controlled_durable_with_epoch(session, query, |before_publish| {
        let entry = db.create_external_table_controlled(entry, before_publish)?;
        let epoch = entry.created_epoch;
        Ok((entry, epoch))
    })?;
    post_commit_result(
        query,
        session
            .ctx
            .register_table(&entry.name, provider)
            .map(|_| ())
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string())),
    )?;
    session.clear_cache();
    Ok(())
}

fn drop_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: &str,
    if_exists: bool,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if db.table_id(name).is_ok() {
        run_controlled_durable_with_epoch(session, query, |before_commit| {
            let epoch = db.drop_table_with_epoch_controlled(name, before_commit)?;
            Ok(((), epoch.0))
        })?;
        let _ = session.ctx.deregister_table(name);
        session.tables.lock().remove(name);
        session.clear_cache();
        return Ok(());
    }
    if let Some(entry) = db.external_table(name) {
        run_controlled_durable_with_epoch(session, query, |before_publish| {
            let epoch = db.drop_external_table_with_epoch_controlled(name, before_publish)?;
            Ok(((), epoch.0))
        })?;
        post_commit_result(
            query,
            session
                .external_modules
                .destroy_external_table(db, &entry, query),
        )?;
        let _ = session.ctx.deregister_table(name);
        session.clear_cache();
        return Ok(());
    }
    if if_exists {
        Ok(())
    } else {
        Err(MongrelQueryError::Schema(format!(
            "table {name:?} not found"
        )))
    }
}

fn drop_view(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: &str,
    if_exists: bool,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if session.view_definition(name).is_none() {
        if if_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "view {name:?} does not exist"
        )));
    }
    let trigger_names = db
        .triggers()
        .into_iter()
        .filter(|trigger| matches!(&trigger.target, TriggerTarget::View(target) if target == name))
        .map(|trigger| trigger.name)
        .collect::<Vec<_>>();
    if !trigger_names.is_empty() {
        run_controlled_durable_with_epoch(session, query, |before_publish| {
            let epoch = db.drop_triggers_with_epoch_controlled(&trigger_names, before_publish)?;
            Ok(((), epoch.0))
        })?;
    }
    session.drop_view(name);
    session.clear_cache();
    Ok(())
}

fn drop_materialized_view(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: &str,
    if_exists: bool,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if db.materialized_view(name).is_none() {
        if if_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "materialized view {name:?} does not exist"
        )));
    }
    run_controlled_durable_with_epoch(session, query, |before_commit| {
        let epoch = db.drop_table_with_epoch_controlled(name, before_commit)?;
        Ok(((), epoch.0))
    })?;
    let _ = session.ctx.deregister_table(name);
    session.tables.lock().remove(name);
    session.clear_cache();
    Ok(())
}

fn external_table_write_error(op: &str, entry: &ExternalTableEntry) -> MongrelQueryError {
    let capability = if entry.capabilities.read_only {
        "read-only"
    } else if entry.capabilities.insert_only && op != "INSERT" {
        "insert-only"
    } else if !entry.capabilities.writable {
        "not writable"
    } else {
        "not wired to the SQL DML path yet"
    };
    MongrelQueryError::Schema(format!(
        "{op} is not supported for external table {:?} using module {:?} ({capability})",
        entry.name, entry.module
    ))
}

fn ensure_external_write_allowed(op: &str, entry: &ExternalTableEntry) -> Result<()> {
    let allowed = match op {
        "INSERT" => {
            !entry.capabilities.read_only
                && (entry.capabilities.writable || entry.capabilities.insert_only)
        }
        "UPDATE" | "DELETE" => !entry.capabilities.read_only && entry.capabilities.writable,
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(external_table_write_error(op, entry))
    }
}

fn refresh_external_table_provider(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    query: Option<&RegisteredSqlQuery>,
) -> Result<()> {
    let provider = session
        .external_modules
        .external_table_provider(db, entry, query)?;
    let _ = session.ctx.deregister_table(&entry.name);
    session
        .ctx
        .register_table(&entry.name, provider)
        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
    session.clear_cache();
    Ok(())
}

fn current_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    query: &RegisteredSqlQuery,
) -> Result<Vec<HashMap<u16, Value>>> {
    query.checkpoint()?;
    if let Some(state) = staged_external_state(session, &entry.name, query)? {
        let rows = session
            .external_modules
            .external_table_rows_from_state(db, entry, &state, query)?;
        crate::external_modules::enforce_external_rows_limit(&rows, Some(query))?;
        query.checkpoint()?;
        return Ok(rows);
    }
    let rows = session
        .external_modules
        .external_table_rows(db, entry, query)?;
    crate::external_modules::enforce_external_rows_limit(&rows, Some(query))?;
    query.checkpoint()?;
    Ok(rows)
}

fn current_external_state(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    query: &RegisteredSqlQuery,
) -> Result<Vec<u8>> {
    query.checkpoint()?;
    if let Some(state) = staged_external_state(session, &entry.name, query)? {
        crate::external_modules::enforce_external_state_limit(&state)?;
        return Ok(state);
    }
    let state = crate::external_modules::external_table_state_bytes(db, entry)?;
    crate::external_modules::enforce_external_state_limit(&state)?;
    query.checkpoint()?;
    Ok(state)
}

fn staged_external_state(
    session: &MongrelSession,
    table_name: &str,
    query: &RegisteredSqlQuery,
) -> Result<Option<Vec<u8>>> {
    let mut transaction = session.transaction.lock();
    let Some(staged) = transaction.staged_ops.as_mut() else {
        return Ok(None);
    };
    let mut state = None;
    for (op_index, op) in staged.reader()?.enumerate() {
        if op_index % COMMAND_CHECKPOINT_ROWS == 0 {
            query.checkpoint()?;
        }
        if let PendingSqlOp::ExternalState {
            table,
            state: candidate,
            ..
        } = op?
        {
            if table == table_name {
                state = Some(candidate);
            }
        }
    }
    Ok(state)
}

fn stage_external_write(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    op: ExternalWriteOp,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let base_state = current_external_state(session, db, entry, query)?;
    query.checkpoint()?;
    let (state, result, base_writes) = session
        .external_modules
        .external_table_write(db, entry, base_state, op, query)?;
    crate::external_modules::enforce_external_state_limit(&state)?;
    query.checkpoint()?;
    let mut ops = base_writes
        .into_iter()
        .map(pending_op_from_external_base_write)
        .collect::<Vec<_>>();
    ops.push(PendingSqlOp::ExternalState {
        table: entry.name.clone(),
        state,
        changes: result.changes,
    });
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

fn pending_op_from_external_base_write(op: ExternalBaseWrite) -> PendingSqlOp {
    match op {
        ExternalBaseWrite::Put { table, cells } => PendingSqlOp::Put { table, cells },
        ExternalBaseWrite::Delete { table, row_id } => PendingSqlOp::Delete {
            table,
            row_id: RowId(row_id),
        },
    }
}

fn insert_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    insert: Insert,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    ensure_external_write_allowed("INSERT", entry)?;
    if insert.on.is_some() {
        return Err(MongrelQueryError::Schema(
            "INSERT conflict actions are not supported for external tables".into(),
        ));
    }
    let schema = &entry.declared_schema;
    let columns = insert_columns(schema, &insert.columns)?;
    let value_rows = values_rows(insert.source.as_deref())?;
    let mut rows = Vec::with_capacity(value_rows.len());
    let mut inserted = 0_u64;
    for (index, value_row) in value_rows.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        if value_row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "INSERT has {} values for {} columns",
                value_row.len(),
                columns.len()
            )));
        }
        let mut row = HashMap::new();
        for (column, expr) in columns.iter().zip(value_row.iter()) {
            row.insert(column.id, expr_to_value(expr, column.ty.clone())?);
        }
        rows.push(row);
        inserted = inserted.saturating_add(1);
    }
    let _ = inserted;
    stage_external_write(session, db, entry, ExternalWriteOp::Insert { rows }, query)
}

fn update_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    update: sqlparser::ast::Update,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    ensure_external_write_allowed("UPDATE", entry)?;
    let schema = &entry.declared_schema;
    let mut rows = current_external_rows(session, db, entry, query)?;
    let mut changed = 0_u64;
    for (index, row) in rows.iter_mut().enumerate() {
        command_checkpoint(session, query, index)?;
        let matches = match update.selection.as_ref() {
            Some(selection) => eval_bool_expr(selection, schema, row, query)?,
            None => true,
        };
        if matches {
            for assignment in &update.assignments {
                apply_assignment(session, schema, row, assignment, None, query)?;
            }
            changed = changed.saturating_add(1);
        }
    }
    stage_external_write(
        session,
        db,
        entry,
        ExternalWriteOp::ReplaceRows {
            rows,
            changes: changed,
        },
        query,
    )
}

fn delete_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    delete: Delete,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    ensure_external_write_allowed("DELETE", entry)?;
    let schema = &entry.declared_schema;
    let rows = current_external_rows(session, db, entry, query)?;
    let mut kept = Vec::with_capacity(rows.len());
    let mut deleted = 0_u64;
    for (index, row) in rows.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        if view_row_matches(delete.selection.as_ref(), schema, &row, query)? {
            deleted = deleted.saturating_add(1);
        } else {
            kept.push(row);
        }
    }
    stage_external_write(
        session,
        db,
        entry,
        ExternalWriteOp::ReplaceRows {
            rows: kept,
            changes: deleted,
        },
        query,
    )
}

fn create_trigger(
    session: &MongrelSession,
    db: &Arc<Database>,
    create: CreateTrigger,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let or_replace = create.or_replace || create.or_alter;
    let trigger = trigger_from_sql(session, db, create, query)?;
    run_controlled_durable_with_epoch(session, query, |before_publish| {
        let trigger = if or_replace {
            db.create_or_replace_trigger_controlled(trigger, before_publish)?
        } else {
            db.create_trigger_controlled(trigger, before_publish)?
        };
        let epoch = trigger.updated_epoch;
        Ok(((), epoch))
    })
}

fn create_trigger_if_not_exists_manual(
    session: &MongrelSession,
    db: &Arc<Database>,
    sql: &str,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let rest = &sql["create trigger if not exists ".len()..];
    let rewritten = format!("create trigger {rest}");
    let statements = Parser::parse_sql(&GenericDialect {}, &rewritten)
        .map_err(|e| MongrelQueryError::Schema(format!("SQL parse error: {e}")))?;
    if statements.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "only one statement may be executed at a time".into(),
        ));
    }
    let Some(statement) = statements.into_iter().next() else {
        return Err(MongrelQueryError::InvalidQueryState(
            "trigger parser returned no statement".into(),
        ));
    };
    let Statement::CreateTrigger(trigger) = statement else {
        return Err(MongrelQueryError::Schema(
            "expected CREATE TRIGGER IF NOT EXISTS".into(),
        ));
    };
    let name = object_name(&trigger.name)?;
    if db.trigger(&name).is_some() {
        return Ok(());
    }
    create_trigger(session, db, trigger, query)?;
    session.clear_cache();
    Ok(())
}

fn drop_trigger(
    session: &MongrelSession,
    db: &Arc<Database>,
    drop: DropTrigger,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if drop.table_name.is_some() {
        return Err(MongrelQueryError::Schema(
            "DROP TRIGGER ON <table> is not required; trigger names are database-scoped".into(),
        ));
    }
    let name = object_name(&drop.trigger_name)?;
    if db.trigger(&name).is_none() {
        if drop.if_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Core(
            mongreldb_core::MongrelError::NotFound(name),
        ));
    }
    run_controlled_durable_with_epoch(session, query, |before_publish| {
        let names = [name.clone()];
        let epoch = db.drop_triggers_with_epoch_controlled(&names, before_publish)?;
        Ok(((), epoch.0))
    })
}

fn trigger_from_sql(
    session: &MongrelSession,
    db: &Arc<Database>,
    create: CreateTrigger,
    query: &RegisteredSqlQuery,
) -> Result<StoredTrigger> {
    if create.temporary || create.is_constraint || create.referenced_table_name.is_some() {
        return Err(MongrelQueryError::Schema(
            "TEMP/CONSTRAINT/FROM triggers are not supported".into(),
        ));
    }
    if !create.referencing.is_empty() || create.characteristics.is_some() {
        return Err(MongrelQueryError::Schema(
            "REFERENCING and trigger characteristics are not supported".into(),
        ));
    }
    if create.exec_body.is_some() {
        return Err(MongrelQueryError::Schema(
            "EXECUTE FUNCTION/PROCEDURE trigger bodies are not supported; use BEGIN ... END statements".into(),
        ));
    }
    match create.trigger_object {
        None | Some(TriggerObjectKind::ForEach(TriggerObject::Row)) => {}
        Some(_) => {
            return Err(MongrelQueryError::Schema(
                "only row-level triggers are supported".into(),
            ));
        }
    }

    let timing = match create.period.unwrap_or(TriggerPeriod::After) {
        TriggerPeriod::After => TriggerTiming::After,
        TriggerPeriod::Before => TriggerTiming::Before,
        TriggerPeriod::InsteadOf => TriggerTiming::InsteadOf,
        TriggerPeriod::For => {
            return Err(MongrelQueryError::Schema(
                "FOR is not a trigger timing".into(),
            ));
        }
    };
    if create.events.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "triggers currently support exactly one event".into(),
        ));
    }
    let (event, update_of) = match &create.events[0] {
        SqlTriggerEvent::Insert => (TriggerEvent::Insert, Vec::new()),
        SqlTriggerEvent::Delete => (TriggerEvent::Delete, Vec::new()),
        SqlTriggerEvent::Update(columns) => (
            TriggerEvent::Update,
            columns.iter().map(|c| c.value.clone()).collect(),
        ),
        SqlTriggerEvent::Truncate => {
            return Err(MongrelQueryError::Schema(
                "TRUNCATE triggers are not supported".into(),
            ));
        }
    };
    let name = object_name(&create.name)?;
    let target_name = object_name(&create.table_name)?;
    let (target, target_schema, target_columns) = match timing {
        TriggerTiming::Before | TriggerTiming::After => {
            if session.view_schema(&target_name).is_some() {
                return Err(MongrelQueryError::Schema(
                    "views support INSTEAD OF triggers, not BEFORE/AFTER triggers".into(),
                ));
            }
            (
                TriggerTarget::Table(target_name.clone()),
                table_schema(db, &target_name)?,
                Vec::new(),
            )
        }
        TriggerTiming::InsteadOf => {
            let schema = session.view_schema(&target_name).ok_or_else(|| {
                MongrelQueryError::Schema(format!(
                    "INSTEAD OF trigger target view {target_name:?} does not exist"
                ))
            })?;
            if schema.columns.is_empty() {
                return Err(MongrelQueryError::Schema(
                    "INSTEAD OF triggers require CREATE VIEW column names".into(),
                ));
            }
            (
                TriggerTarget::View(target_name.clone()),
                schema.clone(),
                schema.columns.clone(),
            )
        }
    };
    let when = create
        .condition
        .as_ref()
        .map(|expr| trigger_expr_from_sql(expr, &target_schema, event))
        .transpose()?;
    let statements = create.statements.ok_or_else(|| {
        MongrelQueryError::Schema("trigger body must contain BEGIN ... END statements".into())
    })?;
    let mut steps = Vec::new();
    for (index, statement) in trigger_statement_list(&statements).iter().enumerate() {
        if index % COMMAND_CHECKPOINT_ROWS == 0 {
            session.fire_test_hook(SqlTestHookPoint::DuringTriggerExpansion);
            query.checkpoint()?;
        }
        steps.extend(trigger_steps_from_statement(
            session,
            db,
            statement,
            &target_schema,
            event,
            query,
        )?);
    }
    let trigger = StoredTrigger::new(
        name,
        TriggerDefinition {
            target,
            timing,
            event,
            update_of,
            target_columns,
            when,
            program: TriggerProgram { steps },
        },
        0,
    )
    .map_err(MongrelQueryError::Core)?;
    Ok(trigger)
}

fn trigger_statement_list(statements: &ConditionalStatements) -> &[Statement] {
    match statements {
        ConditionalStatements::Sequence { statements } => statements,
        ConditionalStatements::BeginEnd(block) => &block.statements,
    }
}

fn trigger_steps_from_statement(
    session: &MongrelSession,
    db: &Arc<Database>,
    statement: &Statement,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
    query: &RegisteredSqlQuery,
) -> Result<Vec<TriggerStep>> {
    match statement {
        Statement::Insert(insert) => {
            trigger_insert_steps(session, db, insert, trigger_schema, event, query)
        }
        Statement::Update(update) => trigger_update_step(db, update, trigger_schema, event),
        Statement::Delete(delete) => trigger_delete_step(db, delete, trigger_schema, event),
        Statement::Query(query) => trigger_query_step(query, trigger_schema, event),
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported trigger body statement: {other}"
        ))),
    }
}

fn trigger_insert_steps(
    session: &MongrelSession,
    db: &Arc<Database>,
    insert: &Insert,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
    query: &RegisteredSqlQuery,
) -> Result<Vec<TriggerStep>> {
    if insert.returning.is_some() || insert.on.is_some() {
        return Err(MongrelQueryError::Schema(
            "trigger INSERT does not support RETURNING or conflict actions".into(),
        ));
    }
    let table = match &insert.table {
        TableObject::TableName(name) => object_name(name)?,
        _ => {
            return Err(MongrelQueryError::Schema(
                "trigger INSERT target must be a table name".into(),
            ));
        }
    };
    let schema = table_schema(db, &table)?;
    let columns = insert_columns(&schema, &insert.columns)?;
    let rows = values_rows(insert.source.as_deref())?;
    let mut steps = Vec::with_capacity(rows.len());
    let mut expanded_values = 0_usize;
    for row in rows {
        if expanded_values.is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
            session.fire_test_hook(SqlTestHookPoint::DuringTriggerExpansion);
            query.checkpoint()?;
        }
        if row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "trigger INSERT has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let mut cells = Vec::with_capacity(row.len());
        for (col, expr) in columns.iter().zip(row.iter()) {
            if expanded_values.is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
                session.fire_test_hook(SqlTestHookPoint::DuringTriggerExpansion);
                query.checkpoint()?;
            }
            cells.push(TriggerCell {
                column_id: col.id,
                value: trigger_value_from_sql(expr, trigger_schema, event, Some(col.ty.clone()))?,
            });
            expanded_values = expanded_values.saturating_add(1);
        }
        steps.push(TriggerStep::Insert {
            table: table.clone(),
            cells,
        });
    }
    Ok(steps)
}

fn trigger_update_step(
    db: &Arc<Database>,
    update: &sqlparser::ast::Update,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    if update.returning.is_some() || update.output.is_some() || update.from.is_some() {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE does not support RETURNING/OUTPUT/FROM".into(),
        ));
    }
    if !update.table.joins.is_empty() {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE with joins is not supported".into(),
        ));
    }
    let table = table_factor_name(&update.table.relation)?;
    let schema = table_schema(db, &table)?;
    let pk = trigger_pk_condition(update.selection.as_ref(), &schema, trigger_schema, event)?;
    let mut cells = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let column_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name(name)?,
            AssignmentTarget::Tuple(_) => {
                return Err(MongrelQueryError::Schema(
                    "trigger UPDATE tuple assignments are not supported".into(),
                ));
            }
        };
        let col = schema
            .column(&column_name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
        cells.push(TriggerCell {
            column_id: col.id,
            value: trigger_value_from_sql(
                &assignment.value,
                trigger_schema,
                event,
                Some(col.ty.clone()),
            )?,
        });
    }
    Ok(vec![TriggerStep::UpdateByPk { table, pk, cells }])
}

fn trigger_delete_step(
    db: &Arc<Database>,
    delete: &Delete,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    let table = single_from_table(&delete.from)?;
    let schema = table_schema(db, &table)?;
    let pk = trigger_pk_condition(delete.selection.as_ref(), &schema, trigger_schema, event)?;
    Ok(vec![TriggerStep::DeleteByPk { table, pk }])
}

fn trigger_query_step(
    query: &Query,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only simple SELECT".into(),
        ));
    };
    if select.projection.len() != 1 || !select.from.is_empty() {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only SELECT RAISE(...)".into(),
        ));
    }
    let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
    else {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only SELECT RAISE(...)".into(),
        ));
    };
    if !object_name(&func.name)?.eq_ignore_ascii_case("raise") {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only SELECT RAISE(...)".into(),
        ));
    }
    let FunctionArguments::List(args) = &func.args else {
        return Err(MongrelQueryError::Schema(
            "RAISE requires argument list".into(),
        ));
    };
    if args.args.is_empty() {
        return Err(MongrelQueryError::Schema("RAISE requires an action".into()));
    }
    let action = raise_action_from_arg(&args.args[0])?;
    let message = if action == TriggerRaiseAction::Ignore {
        TriggerValue::Literal(Value::Null)
    } else {
        let Some(arg) = args.args.get(1) else {
            return Err(MongrelQueryError::Schema(
                "RAISE action requires a message".into(),
            ));
        };
        trigger_value_from_sql(
            function_arg_expr(arg)?,
            trigger_schema,
            event,
            Some(TypeId::Bytes),
        )?
    };
    Ok(vec![TriggerStep::Raise { action, message }])
}

fn raise_action_from_arg(arg: &FunctionArg) -> Result<TriggerRaiseAction> {
    let expr = function_arg_expr(arg)?;
    let name = match expr {
        Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
        Expr::Value(v) => match sql_value_to_value(&v.value, Some(TypeId::Bytes))? {
            Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_ascii_lowercase(),
            _ => {
                return Err(MongrelQueryError::Schema(
                    "RAISE action must be an identifier or string".into(),
                ));
            }
        },
        _ => {
            return Err(MongrelQueryError::Schema(
                "RAISE action must be an identifier or string".into(),
            ));
        }
    };
    match name.as_str() {
        "abort" => Ok(TriggerRaiseAction::Abort),
        "fail" => Ok(TriggerRaiseAction::Fail),
        "rollback" => Ok(TriggerRaiseAction::Rollback),
        "ignore" => Ok(TriggerRaiseAction::Ignore),
        _ => Err(MongrelQueryError::Schema(format!(
            "unsupported RAISE action {name:?}"
        ))),
    }
}

fn function_arg_expr(arg: &FunctionArg) -> Result<&Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        _ => Err(MongrelQueryError::Schema(
            "trigger function arguments must be positional expressions".into(),
        )),
    }
}

fn trigger_pk_condition(
    selection: Option<&Expr>,
    step_schema: &CoreSchema,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<TriggerValue> {
    let pk = step_schema.primary_key().ok_or_else(|| {
        MongrelQueryError::Schema("trigger UPDATE/DELETE target needs a primary key".into())
    })?;
    let Some(Expr::BinaryOp { left, op, right }) = selection else {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE/DELETE requires WHERE pk = expr".into(),
        ));
    };
    if *op != BinaryOperator::Eq {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE/DELETE requires WHERE pk = expr".into(),
        ));
    }
    if expr_is_column(left, &pk.name) {
        trigger_value_from_sql(right, trigger_schema, event, Some(pk.ty.clone()))
    } else if expr_is_column(right, &pk.name) {
        trigger_value_from_sql(left, trigger_schema, event, Some(pk.ty.clone()))
    } else {
        Err(MongrelQueryError::Schema(format!(
            "trigger UPDATE/DELETE WHERE must compare primary key {}",
            pk.name
        )))
    }
}

fn expr_is_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value == column,
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => parts[1].value == column,
        _ => false,
    }
}

fn trigger_expr_from_sql(
    expr: &Expr,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<TriggerExpr> {
    match expr {
        Expr::Nested(inner) => trigger_expr_from_sql(inner, trigger_schema, event),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => Ok(TriggerExpr::Eq {
                left: trigger_value_from_sql(left, trigger_schema, event, None)?,
                right: trigger_value_from_sql(right, trigger_schema, event, None)?,
            }),
            BinaryOperator::NotEq => Ok(TriggerExpr::NotEq {
                left: trigger_value_from_sql(left, trigger_schema, event, None)?,
                right: trigger_value_from_sql(right, trigger_schema, event, None)?,
            }),
            _ => Err(MongrelQueryError::Schema(format!(
                "unsupported trigger WHEN operator {op}"
            ))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => invert_trigger_expr(trigger_expr_from_sql(expr, trigger_schema, event)?),
        Expr::IsNull(inner) => Ok(TriggerExpr::IsNull(trigger_value_from_sql(
            inner,
            trigger_schema,
            event,
            None,
        )?)),
        Expr::IsNotNull(inner) => Ok(TriggerExpr::IsNotNull(trigger_value_from_sql(
            inner,
            trigger_schema,
            event,
            None,
        )?)),
        _ => Ok(TriggerExpr::Value(trigger_value_from_sql(
            expr,
            trigger_schema,
            event,
            Some(TypeId::Bool),
        )?)),
    }
}

fn invert_trigger_expr(expr: TriggerExpr) -> Result<TriggerExpr> {
    match expr {
        TriggerExpr::Eq { left, right } => Ok(TriggerExpr::NotEq { left, right }),
        TriggerExpr::NotEq { left, right } => Ok(TriggerExpr::Eq { left, right }),
        TriggerExpr::Lt { left, right } => Ok(TriggerExpr::Gte { left, right }),
        TriggerExpr::Lte { left, right } => Ok(TriggerExpr::Gt { left, right }),
        TriggerExpr::Gt { left, right } => Ok(TriggerExpr::Lte { left, right }),
        TriggerExpr::Gte { left, right } => Ok(TriggerExpr::Lt { left, right }),
        TriggerExpr::IsNull(value) => Ok(TriggerExpr::IsNotNull(value)),
        TriggerExpr::IsNotNull(value) => Ok(TriggerExpr::IsNull(value)),
        TriggerExpr::And { left, right } => Ok(TriggerExpr::Or {
            left: Box::new(invert_trigger_expr(*left)?),
            right: Box::new(invert_trigger_expr(*right)?),
        }),
        TriggerExpr::Or { left, right } => Ok(TriggerExpr::And {
            left: Box::new(invert_trigger_expr(*left)?),
            right: Box::new(invert_trigger_expr(*right)?),
        }),
        TriggerExpr::Not(inner) => Ok(*inner),
        TriggerExpr::Value(TriggerValue::Literal(Value::Bool(value))) => Ok(TriggerExpr::Value(
            TriggerValue::Literal(Value::Bool(!value)),
        )),
        TriggerExpr::Value(value) => Err(MongrelQueryError::Schema(format!(
            "trigger WHEN NOT only supports comparisons, IS NULL, IS NOT NULL, or boolean literals; got {value:?}"
        ))),
    }
}

fn trigger_value_from_sql(
    expr: &Expr,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
    literal_type: Option<TypeId>,
) -> Result<TriggerValue> {
    match expr {
        Expr::Nested(inner) => trigger_value_from_sql(inner, trigger_schema, event, literal_type),
        Expr::Value(v) => {
            let value = sql_value_to_value(&v.value, literal_type.clone())?;
            let value = match literal_type {
                Some(ty) => coerce_value(value, ty)?,
                None => value,
            };
            Ok(TriggerValue::Literal(value))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match trigger_value_from_sql(expr, trigger_schema, event, literal_type)? {
            TriggerValue::Literal(Value::Int64(v)) => {
                Ok(TriggerValue::Literal(Value::Int64(v.saturating_neg())))
            }
            TriggerValue::Literal(Value::Float64(v)) => {
                Ok(TriggerValue::Literal(Value::Float64(-v)))
            }
            _ => Err(MongrelQueryError::Schema(
                "trigger unary minus requires a numeric literal".into(),
            )),
        },
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let row = parts[0].value.to_ascii_lowercase();
            let column = trigger_schema.column(&parts[1].value).ok_or_else(|| {
                MongrelQueryError::Schema(format!("unknown trigger column {}", parts[1].value))
            })?;
            match row.as_str() {
                "new" => {
                    if event == TriggerEvent::Delete {
                        return Err(MongrelQueryError::Schema(
                            "DELETE triggers cannot reference NEW".into(),
                        ));
                    }
                    Ok(TriggerValue::NewColumn(column.id))
                }
                "old" => {
                    if event == TriggerEvent::Insert {
                        return Err(MongrelQueryError::Schema(
                            "INSERT triggers cannot reference OLD".into(),
                        ));
                    }
                    Ok(TriggerValue::OldColumn(column.id))
                }
                _ => Err(MongrelQueryError::Schema(format!(
                    "unsupported trigger row qualifier {row:?}"
                ))),
            }
        }
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported trigger value expression {other}"
        ))),
    }
}

async fn create_view(
    session: &MongrelSession,
    view: CreateView,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let name = object_name(&view.name)?;

    if view.materialized {
        let Some(db) = &session.database else {
            return Err(MongrelQueryError::Schema(
                "CREATE MATERIALIZED VIEW requires a Database".into(),
            ));
        };
        if view.if_not_exists && db.table_id(&name).is_ok() {
            return Ok(());
        }
        if db.table_id(&name).is_ok() {
            return Err(MongrelQueryError::Schema(format!(
                "table or materialized view {name:?} already exists"
            )));
        }
        create_table_as_select(session, db, &name, &view.query, query, true).await?;
        return Ok(());
    }

    let (schema, input_types) = view_schema_from_columns(&view.columns)?;
    session.create_view_with_schema(&name, &view.query.to_string(), schema, input_types);
    Ok(())
}

async fn refresh_materialized_view(
    session: &MongrelSession,
    name: &str,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let db = session.database.as_ref().ok_or_else(|| {
        MongrelQueryError::Schema("REFRESH MATERIALIZED VIEW requires a Database".into())
    })?;
    if session.transaction.lock().staged_ops.is_some() {
        return Err(MongrelQueryError::Schema(
            "REFRESH MATERIALIZED VIEW is not allowed inside an explicit transaction".into(),
        ));
    }
    let mut definition = db.materialized_view(name).ok_or_else(|| {
        MongrelQueryError::Schema(format!("materialized view {name:?} does not exist"))
    })?;
    if definition.incremental.is_none() {
        let definition_query = Parser::parse_sql(&GenericDialect {}, &definition.query)
            .map_err(|error| MongrelQueryError::Schema(error.to_string()))?
            .into_iter()
            .next()
            .and_then(|statement| match statement {
                Statement::Query(query) => Some(query),
                _ => None,
            })
            .ok_or_else(|| {
                MongrelQueryError::Schema("materialized view definition is not a query".into())
            })?;
        definition.incremental = infer_incremental_aggregate(db, name, &definition_query)?;
        if definition.incremental.is_some() {
            rebuild_incremental_aggregate(session, db, &mut definition, query)?;
            session.clear_cache();
            return Ok(());
        }
    }
    if definition.incremental.is_some() {
        if refresh_incremental_aggregate(session, db, &mut definition, query)?.is_none() {
            rebuild_incremental_aggregate(session, db, &mut definition, query)?;
        }
        session.clear_cache();
        return Ok(());
    }
    let schema = table_schema(db, name)?;
    query.checkpoint()?;
    let mut stream = session
        .execute_command_source_stream(&definition.query, query)
        .await?;
    if stream.schema().fields().len() != schema.columns.len() {
        return Err(MongrelQueryError::Schema(format!(
            "materialized view {name:?} query now returns {} columns; expected {}",
            stream.schema().fields().len(),
            schema.columns.len()
        )));
    }

    let temp_table = format!("__mongreldb_ctas_build_{}_mv_refresh", query.id());
    db.create_rebuilding_table(&temp_table, name, &query.id().to_string(), schema.clone())?;
    let mut converted = 0_usize;
    loop {
        let batch = match next_command_batch(&mut stream, query).await {
            Ok(Some(batch)) => batch,
            Ok(None) => break,
            Err(error) => {
                let _ = db.discard_building_table(&temp_table);
                return Err(error);
            }
        };
        let batch_bytes = batch
            .columns()
            .iter()
            .map(|column| column.get_array_memory_size())
            .fold(0_usize, usize::saturating_add);
        if batch_bytes > CTAS_INPUT_BATCH_BYTES_LIMIT {
            let _ = db.discard_building_table(&temp_table);
            return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "materialized-view refresh input batch bytes",
                requested: batch_bytes,
                limit: CTAS_INPUT_BATCH_BYTES_LIMIT,
            }
            .into());
        }

        let mut row_index = 0_usize;
        while row_index < batch.num_rows() {
            let mut transaction = db.begin_as(session.principal());
            let mut staged_rows = 0_usize;
            let mut staged_bytes = 0_usize;
            while row_index < batch.num_rows() && staged_rows < CTAS_STAGING_ROWS_LIMIT {
                if let Err(error) = command_checkpoint(session, query, converted) {
                    drop(transaction);
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error);
                }
                let mut cells = Vec::with_capacity(batch.num_columns());
                for (column_index, column) in batch.columns().iter().enumerate() {
                    let value = match arrow_cell_to_value(column, row_index) {
                        Ok(value) => value,
                        Err(error) => {
                            drop(transaction);
                            let _ = db.discard_building_table(&temp_table);
                            return Err(error);
                        }
                    };
                    cells.push((schema.columns[column_index].id, value));
                }
                let row_bytes = cells_deep_bytes(&cells);
                if row_bytes > CTAS_STAGING_BYTES_LIMIT {
                    drop(transaction);
                    let _ = db.discard_building_table(&temp_table);
                    return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                        resource: "materialized-view refresh staged row bytes",
                        requested: row_bytes,
                        limit: CTAS_STAGING_BYTES_LIMIT,
                    }
                    .into());
                }
                if staged_rows > 0
                    && staged_bytes.saturating_add(row_bytes) > CTAS_STAGING_BYTES_LIMIT
                {
                    break;
                }
                if let Err(error) = transaction.put_building(&temp_table, cells) {
                    drop(transaction);
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error.into());
                }
                staged_rows += 1;
                staged_bytes = staged_bytes.saturating_add(row_bytes);
                row_index += 1;
                converted += 1;
            }
            if staged_rows > 0 {
                if let Err(error) = transaction.commit_controlled(query.control(), || Ok(())) {
                    let _ = db.discard_building_table(&temp_table);
                    return Err(error.into());
                }
            }
        }
    }
    if let Err(error) = query.checkpoint() {
        let _ = db.discard_building_table(&temp_table);
        return Err(error);
    }
    let publish = run_controlled_durable_with_epoch(session, query, |before_commit| {
        let epoch = db.publish_materialized_rebuilding_table_controlled(
            &temp_table,
            name,
            definition,
            before_commit,
        )?;
        Ok(((), epoch.0))
    });
    if let Err(error) = publish {
        let may_be_published = matches!(
            &error,
            MongrelQueryError::CommitOutcome {
                committed: true,
                ..
            } | MongrelQueryError::OutcomeUnknown { .. }
        );
        if may_be_published {
            let _ = session.ctx.deregister_table(name);
            session.tables.lock().remove(name);
            if let Err(register_error) = register_table(session, db, name) {
                let message = format!("{error}; table registration failed: {register_error}");
                return Err(
                    if matches!(&error, MongrelQueryError::OutcomeUnknown { .. }) {
                        query.outcome_unknown_error(message)
                    } else {
                        query.commit_outcome_error(message)
                    },
                );
            }
            session.clear_cache();
            let _ = db.discard_building_table(&temp_table);
            return Err(error);
        }
        let _ = db.discard_building_table(&temp_table);
        return Err(error);
    }
    let _ = session.ctx.deregister_table(name);
    session.tables.lock().remove(name);
    if let Err(error) = register_table(session, db, name) {
        return Err(query.commit_outcome_error(error.to_string()));
    }
    session.clear_cache();
    Ok(())
}

struct AggregateState {
    group: Value,
    count: i64,
    sums: HashMap<u16, i64>,
}

struct AggregateDelta {
    group: Value,
    count: i64,
    sums: HashMap<u16, i64>,
}

fn aggregate_group_entry_bytes(key: &[u8], group: &Value, output_count: usize) -> usize {
    std::mem::size_of::<Vec<u8>>()
        .saturating_add(key.len())
        .saturating_add(std::mem::size_of::<AggregateState>())
        .saturating_add(value_encoded_key_len(group))
        .saturating_add(output_count.saturating_mul(
            std::mem::size_of::<(u16, i64)>().saturating_add(2 * std::mem::size_of::<usize>()),
        ))
        .saturating_add(4 * std::mem::size_of::<usize>())
}

fn infer_incremental_aggregate(
    db: &Arc<Database>,
    target: &str,
    query: &Query,
) -> Result<Option<mongreldb_core::IncrementalAggregateView>> {
    let target_schema = table_schema(db, target)?;
    infer_incremental_aggregate_with_schema(db, target, query, &target_schema)
}

fn infer_incremental_aggregate_with_schema(
    db: &Arc<Database>,
    target: &str,
    query: &Query,
    target_schema: &CoreSchema,
) -> Result<Option<mongreldb_core::IncrementalAggregateView>> {
    if query.with.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Ok(None);
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.from.len() != 1
        || !select.from[0].joins.is_empty()
        || select.selection.is_some()
        || select.prewhere.is_some()
        || select.having.is_some()
        || !select.lateral_views.is_empty()
        || !select.connect_by.is_empty()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
    {
        return Ok(None);
    }
    let TableFactor::Table {
        name,
        args,
        with_hints,
        version,
        partitions,
        json_path,
        sample,
        index_hints,
        ..
    } = &select.from[0].relation
    else {
        return Ok(None);
    };
    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
    {
        return Ok(None);
    }
    let source_table = object_name(name)?;
    if source_table == target {
        return Ok(None);
    }
    let source_schema = table_schema(db, &source_table)?;
    let sqlparser::ast::GroupByExpr::Expressions(group_by, modifiers) = &select.group_by else {
        return Ok(None);
    };
    if group_by.len() != 1 || !modifiers.is_empty() || select.projection.len() < 2 {
        return Ok(None);
    }
    let Some(group_name) = check_expr_column_name(&group_by[0]) else {
        return Ok(None);
    };
    let Some(first) = select_item_expr(&select.projection[0]) else {
        return Ok(None);
    };
    if check_expr_column_name(first) != Some(group_name) {
        return Ok(None);
    }
    let group_column = source_schema.column(group_name).ok_or_else(|| {
        MongrelQueryError::Schema(format!("unknown aggregate group column {group_name}"))
    })?;
    if group_column.flags.contains(ColumnFlags::NULLABLE)
        || !matches!(
            group_column.ty,
            TypeId::Bool | TypeId::Int64 | TypeId::Bytes
        )
    {
        return Ok(None);
    }
    if target_schema.columns.len() != select.projection.len() {
        return Ok(None);
    }

    let mut outputs = Vec::new();
    let mut count_output_column = None;
    for (index, item) in select.projection.iter().enumerate().skip(1) {
        let Some(Expr::Function(function)) = select_item_expr(item) else {
            return Ok(None);
        };
        if !matches!(function.parameters, FunctionArguments::None)
            || function.filter.is_some()
            || function.null_treatment.is_some()
            || function.over.is_some()
            || !function.within_group.is_empty()
        {
            return Ok(None);
        }
        let FunctionArguments::List(arguments) = &function.args else {
            return Ok(None);
        };
        if arguments.duplicate_treatment.is_some() || !arguments.clauses.is_empty() {
            return Ok(None);
        }
        let output_column = target_schema.columns[index].id;
        let function_name = object_name(&function.name)?.to_ascii_lowercase();
        let kind = match function_name.as_str() {
            "count"
                if arguments.args.len() == 1
                    && matches!(
                        &arguments.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    ) =>
            {
                if count_output_column.replace(output_column).is_some() {
                    return Ok(None);
                }
                mongreldb_core::IncrementalAggregateKind::Count
            }
            "sum" if arguments.args.len() == 1 => {
                let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &arguments.args[0] else {
                    return Ok(None);
                };
                let Some(source_name) = check_expr_column_name(expr) else {
                    return Ok(None);
                };
                let source_column = source_schema.column(source_name).ok_or_else(|| {
                    MongrelQueryError::Schema(format!("unknown aggregate SUM column {source_name}"))
                })?;
                if source_column.flags.contains(ColumnFlags::NULLABLE)
                    || source_column.ty != TypeId::Int64
                {
                    return Ok(None);
                }
                mongreldb_core::IncrementalAggregateKind::Sum {
                    source_column: source_column.id,
                }
            }
            _ => return Ok(None),
        };
        outputs.push(mongreldb_core::IncrementalAggregateOutput {
            output_column,
            kind,
        });
    }
    let Some(count_output_column) = count_output_column else {
        return Ok(None);
    };
    Ok(Some(mongreldb_core::IncrementalAggregateView {
        source_table_id: db.table_id(&source_table)?,
        source_table,
        group_column: group_column.id,
        group_output_column: target_schema.columns[0].id,
        outputs,
        count_output_column,
        checkpoint_event_id: format!("{}:{}", db.visible_epoch().0, u32::MAX),
    }))
}

fn select_item_expr(item: &sqlparser::ast::SelectItem) -> Option<&Expr> {
    match item {
        sqlparser::ast::SelectItem::UnnamedExpr(expr)
        | sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => Some(expr),
        _ => None,
    }
}

fn check_expr_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(identifier) => Some(&identifier.value),
        Expr::CompoundIdentifier(parts) => parts.last().map(|identifier| identifier.value.as_str()),
        _ => None,
    }
}

fn collect_incremental_aggregate_groups(
    session: &MongrelSession,
    db: &Arc<Database>,
    plan: &mongreldb_core::IncrementalAggregateView,
    query: &RegisteredSqlQuery,
) -> Result<(
    std::collections::BTreeMap<Vec<u8>, AggregateState>,
    mongreldb_core::Snapshot,
)> {
    query.checkpoint()?;
    let (snapshot, _retention) = db.snapshot_owned();
    let mut groups = std::collections::BTreeMap::<Vec<u8>, AggregateState>::new();
    let mut group_state_bytes = 0_usize;
    for_each_visible_row_at_snapshot_controlled(
        session,
        db,
        &plan.source_table,
        Some(snapshot),
        query,
        |_schema, row| {
            let group = row
                .columns
                .get(&plan.group_column)
                .cloned()
                .ok_or_else(|| {
                    MongrelQueryError::Schema("incremental group column is missing".into())
                })?;
            if matches!(group, Value::Null) {
                return Err(MongrelQueryError::Schema(
                    "incremental group column cannot be NULL".into(),
                ));
            }
            let group_key = group.encode_key();
            if !groups.contains_key(&group_key) && groups.len() >= INCREMENTAL_AGGREGATE_MAX_GROUPS
            {
                return Err(MongrelQueryError::Schema(format!(
                    "incremental materialized view exceeds limit of {INCREMENTAL_AGGREGATE_MAX_GROUPS} groups"
                )));
            }
            if !groups.contains_key(&group_key) {
                let requested = group_state_bytes.saturating_add(aggregate_group_entry_bytes(
                    &group_key,
                    &group,
                    plan.outputs.len(),
                ));
                if requested > INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT {
                    return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                        resource: "incremental materialized view group state bytes",
                        requested,
                        limit: INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT,
                    }
                    .into());
                }
                group_state_bytes = requested;
            }
            let state = groups.entry(group_key).or_insert_with(|| AggregateState {
                group,
                count: 0,
                sums: HashMap::new(),
            });
            state.count = state
                .count
                .checked_add(1)
                .ok_or_else(|| MongrelQueryError::Schema("COUNT overflow".into()))?;
            for output in &plan.outputs {
                if let mongreldb_core::IncrementalAggregateKind::Sum { source_column } = output.kind
                {
                    let Some(Value::Int64(value)) = row.columns.get(&source_column) else {
                        return Err(MongrelQueryError::Schema(
                            "incremental SUM column must be non-null BIGINT".into(),
                        ));
                    };
                    let sum = state.sums.entry(source_column).or_insert(0);
                    *sum = sum
                        .checked_add(*value)
                        .ok_or_else(|| MongrelQueryError::Schema("SUM overflow".into()))?;
                }
            }
            Ok(())
        },
    )?;
    Ok((groups, snapshot))
}

fn rebuild_incremental_aggregate(
    session: &MongrelSession,
    db: &Arc<Database>,
    definition: &mut mongreldb_core::MaterializedViewEntry,
    query: &RegisteredSqlQuery,
) -> Result<mongreldb_core::Epoch> {
    let mut plan = definition.incremental.clone().ok_or_else(|| {
        MongrelQueryError::Schema("materialized view has no incremental plan".into())
    })?;
    let (groups, snapshot) = collect_incremental_aggregate_groups(session, db, &plan, query)?;

    plan.checkpoint_event_id = format!("{}:{}", snapshot.epoch.0, u32::MAX);
    definition.incremental = Some(plan.clone());
    let mut transaction = db.begin_as(session.principal());
    transaction.truncate(&definition.name)?;
    for (index, state) in groups.into_values().enumerate() {
        command_checkpoint(session, query, index)?;
        transaction.put(
            &definition.name,
            aggregate_cells(&plan, state.group, state.count, &state.sums)?,
        )?;
    }
    transaction.set_materialized_view_definition(definition.clone())?;
    let epoch = run_controlled_durable_with_epoch(session, query, |before_commit| {
        let epoch = transaction.commit_controlled(query.control(), before_commit)?;
        Ok((epoch, epoch.0))
    })?;
    definition.last_refresh_epoch = epoch.0;
    Ok(epoch)
}

fn refresh_incremental_aggregate(
    session: &MongrelSession,
    db: &Arc<Database>,
    definition: &mut mongreldb_core::MaterializedViewEntry,
    query: &RegisteredSqlQuery,
) -> Result<Option<mongreldb_core::Epoch>> {
    let mut plan = definition.incremental.clone().ok_or_else(|| {
        MongrelQueryError::Schema("materialized view has no incremental plan".into())
    })?;
    query.checkpoint()?;
    let changes =
        db.change_events_since_controlled(Some(&plan.checkpoint_event_id), query.control())?;
    query.checkpoint()?;
    if changes.gap {
        return Ok(None);
    }
    let mut deltas = std::collections::BTreeMap::<Vec<u8>, AggregateDelta>::new();
    let mut delta_state_bytes = 0_usize;
    for (event_index, event) in changes
        .events
        .iter()
        .filter(|event| event.table_id == Some(plan.source_table_id))
        .enumerate()
    {
        command_checkpoint(session, query, event_index)?;
        let rows = match event.op.as_str() {
            "put" => event
                .data
                .clone()
                .and_then(|value| serde_json::from_value::<Vec<Row>>(value).ok()),
            "put_run" => event
                .data
                .as_ref()
                .and_then(|value| value.get("rows"))
                .cloned()
                .and_then(|value| serde_json::from_value::<Vec<Row>>(value).ok()),
            "delete" => {
                let Some(data) = event.data.as_ref() else {
                    return Ok(None);
                };
                let expected = data
                    .get("row_ids")
                    .and_then(serde_json::Value::as_array)
                    .map(Vec::len)
                    .unwrap_or(0);
                let rows = data
                    .get("before")
                    .cloned()
                    .and_then(|value| serde_json::from_value::<Vec<Row>>(value).ok());
                if rows.as_ref().map(Vec::len) != Some(expected) {
                    return Ok(None);
                }
                rows
            }
            "truncate" => return Ok(None),
            _ => Some(Vec::new()),
        };
        let Some(rows) = rows else {
            return Ok(None);
        };
        let sign = if event.op == "delete" { -1 } else { 1 };
        for (row_index, row) in rows.iter().enumerate() {
            command_checkpoint(session, query, row_index)?;
            if !apply_aggregate_delta(&mut deltas, &mut delta_state_bytes, &plan, row, sign)? {
                return Ok(None);
            }
        }
    }

    let mut existing = HashMap::with_capacity(deltas.len());
    let mut existing_bytes = 0_usize;
    for_each_visible_row_controlled(session, db, &definition.name, query, |_schema, row| {
        if let Some(key) = row
            .columns
            .get(&plan.group_output_column)
            .map(Value::encode_key)
        {
            if deltas.contains_key(&key) {
                if !existing.contains_key(&key) {
                    let requested = existing_bytes
                        .saturating_add(std::mem::size_of::<Vec<u8>>())
                        .saturating_add(key.len())
                        .saturating_add(row_deep_bytes(&row));
                    if requested > INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT {
                        return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                            resource: "incremental materialized view existing state bytes",
                            requested,
                            limit: INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT,
                        }
                        .into());
                    }
                    existing_bytes = requested;
                }
                existing.insert(key, row);
            }
        }
        Ok(())
    })?;
    let mut deletes = Vec::new();
    let mut updates = Vec::new();
    let mut inserts = Vec::new();
    let mut mutation_bytes = 0_usize;
    for (index, (key, delta)) in deltas.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        let current = existing.get(&key);
        let current_count = match current.and_then(|row| row.columns.get(&plan.count_output_column))
        {
            Some(Value::Int64(count)) => *count,
            Some(_) => return Ok(None),
            None => 0,
        };
        let Some(next_count) = current_count.checked_add(delta.count) else {
            return Ok(None);
        };
        if next_count < 0 {
            return Ok(None);
        }
        if next_count == 0 {
            if let Some(row) = current {
                deletes.push(row.row_id);
            }
            continue;
        }
        let mut sums = HashMap::new();
        for output in &plan.outputs {
            let mongreldb_core::IncrementalAggregateKind::Sum { source_column } = output.kind
            else {
                continue;
            };
            let current_sum = match current.and_then(|row| row.columns.get(&output.output_column)) {
                Some(Value::Int64(sum)) => *sum,
                Some(_) => return Ok(None),
                None => 0,
            };
            let Some(next_sum) =
                current_sum.checked_add(delta.sums.get(&source_column).copied().unwrap_or(0))
            else {
                return Ok(None);
            };
            sums.insert(source_column, next_sum);
        }
        let cells = aggregate_cells(&plan, delta.group, next_count, &sums)?;
        let requested = mutation_bytes.saturating_add(cells_deep_bytes(&cells));
        if requested > INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT {
            return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "incremental materialized view mutation bytes",
                requested,
                limit: INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT,
            }
            .into());
        }
        mutation_bytes = requested;
        if let Some(row) = current {
            updates.push((row.row_id, cells));
        } else {
            inserts.push(cells);
        }
    }

    plan.checkpoint_event_id = format!("{}:{}", changes.current_epoch, u32::MAX);
    definition.incremental = Some(plan);
    let mut transaction = db.begin_as(session.principal());
    for (index, row_id) in deletes.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        transaction.delete(&definition.name, row_id)?;
    }
    if !updates.is_empty() {
        query.checkpoint()?;
        transaction.update_many(&definition.name, updates)?;
        query.checkpoint()?;
    }
    for (index, cells) in inserts.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        transaction.put(&definition.name, cells)?;
    }
    transaction.set_materialized_view_definition(definition.clone())?;
    let epoch = run_controlled_durable_with_epoch(session, query, |before_commit| {
        let epoch = transaction.commit_controlled(query.control(), before_commit)?;
        Ok((epoch, epoch.0))
    })?;
    definition.last_refresh_epoch = epoch.0;
    Ok(Some(epoch))
}

fn apply_aggregate_delta(
    deltas: &mut std::collections::BTreeMap<Vec<u8>, AggregateDelta>,
    delta_state_bytes: &mut usize,
    plan: &mongreldb_core::IncrementalAggregateView,
    row: &Row,
    sign: i64,
) -> Result<bool> {
    let Some(group) = row.columns.get(&plan.group_column).cloned() else {
        return Ok(false);
    };
    if matches!(group, Value::Null) {
        return Ok(false);
    }
    let group_key = group.encode_key();
    if !deltas.contains_key(&group_key) && deltas.len() >= INCREMENTAL_AGGREGATE_MAX_GROUPS {
        return Ok(false);
    }
    if !deltas.contains_key(&group_key) {
        let requested = delta_state_bytes.saturating_add(aggregate_group_entry_bytes(
            &group_key,
            &group,
            plan.outputs.len(),
        ));
        if requested > INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT {
            return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "incremental materialized view delta state bytes",
                requested,
                limit: INCREMENTAL_AGGREGATE_STATE_BYTES_LIMIT,
            }
            .into());
        }
        *delta_state_bytes = requested;
    }
    let delta = deltas.entry(group_key).or_insert_with(|| AggregateDelta {
        group,
        count: 0,
        sums: HashMap::new(),
    });
    let Some(count) = delta.count.checked_add(sign) else {
        return Ok(false);
    };
    delta.count = count;
    for output in &plan.outputs {
        let mongreldb_core::IncrementalAggregateKind::Sum { source_column } = output.kind else {
            continue;
        };
        let Some(Value::Int64(value)) = row.columns.get(&source_column) else {
            return Ok(false);
        };
        let Some(signed) = value.checked_mul(sign) else {
            return Ok(false);
        };
        let sum = delta.sums.entry(source_column).or_insert(0);
        let Some(next) = sum.checked_add(signed) else {
            return Ok(false);
        };
        *sum = next;
    }
    Ok(true)
}

fn aggregate_cells(
    plan: &mongreldb_core::IncrementalAggregateView,
    group: Value,
    count: i64,
    sums: &HashMap<u16, i64>,
) -> Result<Vec<(u16, Value)>> {
    let mut cells = vec![(plan.group_output_column, group)];
    for output in &plan.outputs {
        let value = match output.kind {
            mongreldb_core::IncrementalAggregateKind::Count => Value::Int64(count),
            mongreldb_core::IncrementalAggregateKind::Sum { source_column } => {
                Value::Int64(sums.get(&source_column).copied().ok_or_else(|| {
                    MongrelQueryError::Schema("incremental SUM state is missing".into())
                })?)
            }
        };
        cells.push((output.output_column, value));
    }
    Ok(cells)
}

fn view_schema_from_columns(
    columns: &[sqlparser::ast::ViewColumnDef],
) -> Result<(CoreSchema, HashMap<u16, Option<TypeId>>)> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(columns.len());
    let mut input_types = HashMap::new();
    for (idx, column) in columns.iter().enumerate() {
        let name = column.name.value.clone();
        if !seen.insert(name.clone()) {
            return Err(MongrelQueryError::Schema(format!(
                "duplicate view column {name}"
            )));
        }
        let id = u16::try_from(idx + 1).map_err(|_| {
            MongrelQueryError::Schema("view has too many columns for trigger routing".into())
        })?;
        let ty = match &column.data_type {
            Some(data_type) => sql_type_to_core(data_type)?,
            None => TypeId::Bytes,
        };
        input_types.insert(
            id,
            column
                .data_type
                .as_ref()
                .map(sql_type_to_core)
                .transpose()?,
        );
        out.push(CoreColumnDef {
            id,
            name,
            ty,
            flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            default_value: None,
            embedding_source: None,
        });
    }
    Ok((
        CoreSchema {
            columns: out,
            ..CoreSchema::default()
        },
        input_types,
    ))
}

fn create_policy(
    session: &MongrelSession,
    db: &Arc<Database>,
    policy: CreatePolicy,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    db.require_for(
        session.principal().as_ref(),
        &mongreldb_core::Permission::Admin,
    )?;
    let table = object_name(&policy.table_name)?;
    let schema = table_schema(db, &table)?;
    let command = match policy.command.unwrap_or(CreatePolicyCommand::All) {
        CreatePolicyCommand::All => mongreldb_core::PolicyCommand::All,
        CreatePolicyCommand::Select => mongreldb_core::PolicyCommand::Select,
        CreatePolicyCommand::Insert => mongreldb_core::PolicyCommand::Insert,
        CreatePolicyCommand::Update => mongreldb_core::PolicyCommand::Update,
        CreatePolicyCommand::Delete => mongreldb_core::PolicyCommand::Delete,
    };
    let subjects = policy
        .to
        .unwrap_or_else(|| vec![Owner::Ident(Ident::new("public"))])
        .into_iter()
        .map(|owner| policy_subject(session, db, owner))
        .collect::<Result<Vec<_>>>()?;
    let using = policy
        .using
        .as_ref()
        .map(|expression| lower_security_expr(expression, &schema))
        .transpose()?
        .or(Some(mongreldb_core::SecurityExpr::True));
    let with_check = policy
        .with_check
        .as_ref()
        .map(|expression| lower_security_expr(expression, &schema))
        .transpose()?;
    let mut security = db.security_catalog();
    if security
        .policies
        .iter()
        .any(|existing| existing.table == table && existing.name == policy.name.value)
    {
        return Err(MongrelQueryError::Schema(format!(
            "policy {} already exists on {table}",
            policy.name.value
        )));
    }
    security.policies.push(mongreldb_core::RowPolicy {
        name: policy.name.value,
        table: table.clone(),
        command,
        subjects,
        permissive: !matches!(policy.policy_type, Some(CreatePolicyType::Restrictive)),
        using,
        with_check,
    });
    run_controlled_durable_with_epoch(session, query, |before_publish| {
        let epoch = db.set_security_catalog_as_with_epoch_controlled(
            security,
            session.principal().as_ref(),
            before_publish,
        )?;
        Ok(((), epoch.0))
    })?;
    post_commit_result(query, session.refresh_registered_table(db, &table))?;
    session.clear_cache();
    Ok(())
}

fn drop_policy(
    session: &MongrelSession,
    db: &Arc<Database>,
    policy: DropPolicy,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    db.require_for(
        session.principal().as_ref(),
        &mongreldb_core::Permission::Admin,
    )?;
    let table = object_name(&policy.table_name)?;
    let mut security = db.security_catalog();
    let old_len = security.policies.len();
    security
        .policies
        .retain(|existing| existing.table != table || existing.name != policy.name.value);
    if security.policies.len() == old_len {
        if policy.if_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "policy {} does not exist on {table}",
            policy.name.value
        )));
    }
    run_controlled_durable_with_epoch(session, query, |before_publish| {
        let epoch = db.set_security_catalog_as_with_epoch_controlled(
            security,
            session.principal().as_ref(),
            before_publish,
        )?;
        Ok(((), epoch.0))
    })?;
    post_commit_result(query, session.refresh_registered_table(db, &table))?;
    session.clear_cache();
    Ok(())
}

fn policy_subject(session: &MongrelSession, db: &Arc<Database>, owner: Owner) -> Result<String> {
    match owner {
        Owner::Ident(ident) => Ok(ident.value),
        Owner::CurrentUser | Owner::SessionUser => session
            .principal()
            .or_else(|| db.principal_snapshot())
            .map(|principal| principal.username)
            .ok_or(MongrelQueryError::Core(
                mongreldb_core::MongrelError::AuthRequired,
            )),
        Owner::CurrentRole => Err(MongrelQueryError::Schema(
            "CURRENT_ROLE is ambiguous when multiple roles are active".into(),
        )),
    }
}

fn lower_security_expr(expr: &Expr, schema: &CoreSchema) -> Result<mongreldb_core::SecurityExpr> {
    use mongreldb_core::SecurityExpr;
    match expr {
        Expr::Nested(expression) => lower_security_expr(expression, schema),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(SecurityExpr::Not {
            expression: Box::new(lower_security_expr(expr, schema)?),
        }),
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
        {
            let left = Box::new(lower_security_expr(left, schema)?);
            let right = Box::new(lower_security_expr(right, schema)?);
            if matches!(op, BinaryOperator::And) {
                Ok(SecurityExpr::And { left, right })
            } else {
                Ok(SecurityExpr::Or { left, right })
            }
        }
        Expr::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::Eq | BinaryOperator::NotEq) =>
        {
            let equality = lower_security_equality(left, right, schema)
                .or_else(|_| lower_security_equality(right, left, schema))?;
            if matches!(op, BinaryOperator::NotEq) {
                Ok(SecurityExpr::Not {
                    expression: Box::new(equality),
                })
            } else {
                Ok(equality)
            }
        }
        Expr::Value(value) if matches!(value.value, SqlValue::Boolean(true)) => {
            Ok(SecurityExpr::True)
        }
        Expr::Value(value) if matches!(value.value, SqlValue::Boolean(false)) => {
            Ok(SecurityExpr::Not {
                expression: Box::new(SecurityExpr::True),
            })
        }
        _ => Err(MongrelQueryError::Schema(format!(
            "unsupported policy expression {expr}; use column = literal/CURRENT_USER with AND, OR, NOT"
        ))),
    }
}

fn lower_security_equality(
    column: &Expr,
    value: &Expr,
    schema: &CoreSchema,
) -> Result<mongreldb_core::SecurityExpr> {
    let column_name = match column {
        Expr::Identifier(ident) => &ident.value,
        Expr::CompoundIdentifier(idents) => {
            &idents
                .last()
                .ok_or_else(|| MongrelQueryError::Schema("empty policy column".into()))?
                .value
        }
        _ => {
            return Err(MongrelQueryError::Schema(
                "policy equality requires a column".into(),
            ))
        }
    };
    let column = schema
        .column(column_name)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown policy column {column_name}")))?;
    if is_current_user_expr(value) {
        if !matches!(column.ty, TypeId::Bytes | TypeId::Enum { .. }) {
            return Err(MongrelQueryError::Schema(
                "CURRENT_USER requires a string/bytes policy column".into(),
            ));
        }
        return Ok(mongreldb_core::SecurityExpr::ColumnEqCurrentUser { column: column.id });
    }
    let value = expr_to_value(value, column.ty.clone())?;
    if matches!(value, Value::Null) {
        return Err(MongrelQueryError::Schema(
            "policy equality with NULL is not supported".into(),
        ));
    }
    Ok(mongreldb_core::SecurityExpr::ColumnEqValue {
        column: column.id,
        value,
    })
}

fn is_current_user_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Function(function)
            if function.name.to_string().eq_ignore_ascii_case("current_user")
                || function.name.to_string().eq_ignore_ascii_case("session_user")
                || function.name.to_string().eq_ignore_ascii_case("user")
    )
}

fn alter_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    alter: AlterTable,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let table_name = object_name(&alter.name)?;
    if alter.operations.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "ALTER TABLE currently supports one operation per statement".into(),
        ));
    }
    let operation = alter.operations.into_iter().next().ok_or_else(|| {
        MongrelQueryError::InvalidQueryState("ALTER TABLE parser returned no operation".into())
    })?;
    match operation {
        AlterTableOperation::RenameTable {
            table_name: new_name,
        } => {
            let new_name = match new_name {
                RenameTableNameKind::As(n) | RenameTableNameKind::To(n) => object_name(&n)?,
            };
            if new_name == table_name {
                return Ok(());
            }
            run_controlled_durable_with_epoch(session, query, |before_commit| {
                let epoch =
                    db.rename_table_with_epoch_controlled(&table_name, &new_name, before_commit)?;
                Ok(((), epoch.0))
            })?;
            let _ = session.ctx.deregister_table(&table_name);
            session.tables.lock().remove(&table_name);
            post_commit_result(query, register_table(session, db, &new_name))?;
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            alter_column_controlled(
                session,
                db,
                &table_name,
                &old_column_name.value,
                AlterColumn::rename(new_column_name.value),
                query,
            )?;
            post_commit_result(query, session.refresh_registered_table(db, &table_name))?;
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            alter_column(session, db, &table_name, column_name, op, query)?;
        }
        AlterTableOperation::AddColumn {
            if_not_exists,
            column_def,
            ..
        } => {
            let mut schema = table_schema(db, &table_name)?;
            if schema.column(&column_def.name.value).is_some() {
                if if_not_exists {
                    return Ok(());
                }
                return Err(MongrelQueryError::Schema(format!(
                    "column {} already exists",
                    column_def.name.value
                )));
            }
            let next_id = schema.columns.iter().map(|c| c.id).max().unwrap_or(0) + 1;
            let col = core_column_from_sql(next_id, &column_def, false)?;
            schema.columns.push(col);
            schema.validate_auto_increment()?;
            rebuild_table(session, db, &table_name, schema, query)?;
        }
        AlterTableOperation::DropColumn {
            column_names,
            if_exists,
            ..
        } => {
            let mut schema = table_schema(db, &table_name)?;
            for column in column_names {
                let old_len = schema.columns.len();
                schema.columns.retain(|c| c.name != column.value);
                if schema.columns.len() == old_len && !if_exists {
                    return Err(MongrelQueryError::Schema(format!(
                        "column {} does not exist",
                        column.value
                    )));
                }
                schema
                    .indexes
                    .retain(|idx| schema.columns.iter().any(|col| col.id == idx.column_id));
            }
            rebuild_table(session, db, &table_name, schema, query)?;
        }
        AlterTableOperation::DropIndex { name } => {
            let mut schema = table_schema(db, &table_name)?;
            remove_index_defs(&mut schema, &name.value);
            rebuild_table(session, db, &table_name, schema, query)?;
        }
        AlterTableOperation::AddConstraint { constraint, .. } => {
            add_table_constraint(session, db, &table_name, constraint, query)?;
        }
        AlterTableOperation::DropConstraint {
            name, if_exists, ..
        } => {
            let mut schema = table_schema(db, &table_name)?;
            let old_len = schema.indexes.len();
            remove_index_defs(&mut schema, &name.value);
            if schema.indexes.len() == old_len && !if_exists {
                return Err(MongrelQueryError::Schema(format!(
                    "constraint/index {} does not exist",
                    name.value
                )));
            }
            rebuild_table(session, db, &table_name, schema, query)?;
        }
        AlterTableOperation::EnableRowLevelSecurity => {
            let mut security = db.security_catalog();
            if security.rls_tables.contains(&table_name) {
                return Ok(());
            }
            security.rls_tables.push(table_name.clone());
            run_controlled_durable_with_epoch(session, query, |before_publish| {
                let epoch = db.set_security_catalog_as_with_epoch_controlled(
                    security,
                    session.principal().as_ref(),
                    before_publish,
                )?;
                Ok(((), epoch.0))
            })?;
            post_commit_result(query, session.refresh_registered_table(db, &table_name))?;
        }
        AlterTableOperation::DisableRowLevelSecurity => {
            let mut security = db.security_catalog();
            if !security.rls_tables.contains(&table_name) {
                return Ok(());
            }
            security.rls_tables.retain(|table| table != &table_name);
            run_controlled_durable_with_epoch(session, query, |before_publish| {
                let epoch = db.set_security_catalog_as_with_epoch_controlled(
                    security,
                    session.principal().as_ref(),
                    before_publish,
                )?;
                Ok(((), epoch.0))
            })?;
            post_commit_result(query, session.refresh_registered_table(db, &table_name))?;
        }
        other => {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported ALTER TABLE operation: {other}"
            )));
        }
    }
    session.clear_cache();
    Ok(())
}

fn alter_column(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    column: Ident,
    op: AlterColumnOperation,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let change = match op {
        AlterColumnOperation::SetNotNull => {
            let flags =
                current_column_flags(db, table, &column.value)?.without(ColumnFlags::NULLABLE);
            AlterColumn::set_flags(flags)
        }
        AlterColumnOperation::DropNotNull => {
            let flags = current_column_flags(db, table, &column.value)?.with(ColumnFlags::NULLABLE);
            AlterColumn::set_flags(flags)
        }
        AlterColumnOperation::SetDataType { data_type, .. } => {
            AlterColumn::set_type(sql_type_to_core(&data_type)?)
        }
        AlterColumnOperation::SetDefault { value, .. } => {
            AlterColumn::set_default(sql_expr_to_default(&value)?)
        }
        AlterColumnOperation::DropDefault => AlterColumn::drop_default(),
        other => {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported ALTER COLUMN operation: {other}"
            )));
        }
    };
    alter_column_controlled(session, db, table, &column.value, change, query)?;
    post_commit_result(query, session.refresh_registered_table(db, table))?;
    Ok(())
}

fn alter_column_controlled(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    column: &str,
    change: AlterColumn,
    query: &RegisteredSqlQuery,
) -> Result<Option<mongreldb_core::Epoch>> {
    let outcome_unknown = std::cell::Cell::new(false);
    let result = db.alter_column_with_epoch_controlled(
        table,
        column,
        change,
        query.control(),
        || enter_commit_fence(session, query).map_err(query_error_to_core),
        |epoch| match epoch {
            Some(epoch) => {
                query.record_commit(query.status().statement_index, epoch.0);
                let exit = query.exit_commit_critical().map_err(query_error_to_core);
                session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
                exit
            }
            None => {
                outcome_unknown.set(true);
                query.mark_outcome_unknown();
                query.exit_commit_critical().map_err(query_error_to_core)
            }
        },
    );
    match result {
        Ok((_, epoch)) => Ok(epoch),
        Err(mongreldb_core::MongrelError::DurableCommit { epoch, message }) => {
            if query.status().durable_outcome.last_commit_epoch != Some(epoch) {
                query.record_commit(query.status().statement_index, epoch);
            }
            if query.status().phase == SqlQueryPhase::CommitCritical {
                let exit = query.exit_commit_critical();
                session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
                if let Err(error) = exit {
                    return Err(query.commit_outcome_error(format!("{message}; {error}")));
                }
            }
            query.checkpoint()?;
            Err(query.commit_outcome_error(message))
        }
        Err(error) if outcome_unknown.get() || query.status().outcome_unknown => {
            Err(query.outcome_unknown_error(error.to_string()))
        }
        Err(error) => {
            query.checkpoint()?;
            Err(error.into())
        }
    }
}

fn add_table_constraint(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    constraint: TableConstraint,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    match constraint {
        TableConstraint::Index(idx) => {
            let mut schema = table_schema(db, table)?;
            let name = idx.name.map(|n| n.value).unwrap_or_else(|| {
                idx.columns
                    .first()
                    .map(|c| format!("idx_{}", c.column.expr))
                    .unwrap_or_else(|| "idx".to_string())
            });
            add_index_defs(
                &mut schema,
                &name,
                idx.columns,
                index_kind_from_sql(idx.index_type.as_ref())?,
            )?;
            rebuild_table(session, db, table, schema, query)
        }
        TableConstraint::FulltextOrSpatial(idx) => {
            if !idx.fulltext {
                return Err(MongrelQueryError::Schema(
                    "SPATIAL indexes are not supported".into(),
                ));
            }
            let mut schema = table_schema(db, table)?;
            let name = idx
                .opt_index_name
                .map(|n| n.value)
                .unwrap_or_else(|| "fulltext_idx".to_string());
            add_index_defs(&mut schema, &name, idx.columns, IndexKind::FmIndex)?;
            rebuild_table(session, db, table, schema, query)
        }
        TableConstraint::PrimaryKey(pk) => Err(MongrelQueryError::Schema(format!(
            "adding primary keys after table creation is not supported: {pk}"
        ))),
        TableConstraint::Check(check) => {
            if check.enforced == Some(false) {
                return Err(MongrelQueryError::Schema(
                    "NOT ENFORCED CHECK constraints are not supported".into(),
                ));
            }
            let mut schema = table_schema(db, table)?;
            let expr = lower_check_expr(&check.expr, &schema)?;
            expr.validate()?;
            let id = (schema.constraints.checks.len() + 1) as u16;
            schema.constraints.checks.push(CoreCheckConstraint {
                id,
                name: check
                    .name
                    .map(|name| name.value)
                    .unwrap_or_else(|| format!("check_{id}")),
                expr,
            });
            rebuild_table(session, db, table, schema, query)
        }
        TableConstraint::Unique(_)
        | TableConstraint::ForeignKey(_)
        | TableConstraint::PrimaryKeyUsingIndex(_)
        | TableConstraint::UniqueUsingIndex(_) => Err(MongrelQueryError::Schema(
            "UNIQUE and FOREIGN KEY enforcement is provided by MongrelDB Kit, not core SQL DDL"
                .into(),
        )),
    }
}

fn create_index(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: CreateIndex,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if index.unique {
        return Err(MongrelQueryError::Schema(
            "CREATE UNIQUE INDEX is not supported by core SQL; use MongrelDB Kit unique constraints".into(),
        ));
    }
    // Serialize the partial-index predicate (if any) as a SQL string.
    let predicate = index.predicate.as_ref().map(|expr| expr.to_string());
    let table = object_name(&index.table_name)?;
    let mut schema = table_schema(db, &table)?;
    let name = index
        .name
        .as_ref()
        .map(object_name)
        .transpose()?
        .unwrap_or_else(|| {
            index
                .columns
                .first()
                .map(|c| format!("idx_{}", c.column.expr))
                .unwrap_or_else(|| "idx".into())
        });
    if schema.indexes.iter().any(|idx| idx.name == name) {
        if index.if_not_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "index {name} already exists on {table}"
        )));
    }
    let kind = index_kind_from_sql(index.using.as_ref())?;
    let options = parse_index_options(kind, &index.with)?;
    add_index_defs(&mut schema, &name, index.columns, kind)?;
    // Attach the predicate to the newly created index defs.
    if let Some(pred) = &predicate {
        for idx in schema.indexes.iter_mut() {
            if idx.name.starts_with(&name) {
                idx.predicate = Some(pred.clone());
            }
        }
    }
    for idx in schema.indexes.iter_mut() {
        if idx.name == name || idx.name.starts_with(&format!("{name}_")) {
            idx.options = options.clone();
            idx.validate_options()?;
        }
    }
    rebuild_table(session, db, &table, schema, query)?;
    session.clear_cache();
    Ok(())
}

fn drop_index(
    session: &MongrelSession,
    db: &Arc<Database>,
    names: Vec<ObjectName>,
    table: Option<ObjectName>,
    if_exists: bool,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    for name in names {
        let index_name = object_name(&name)?;
        let table_name = match &table {
            Some(t) => object_name(t)?,
            None => find_index_table(db, &index_name)?.ok_or_else(|| {
                MongrelQueryError::Schema(format!(
                    "DROP INDEX {index_name} requires ON <table> when the index cannot be resolved"
                ))
            })?,
        };
        let mut schema = table_schema(db, &table_name)?;
        let old_len = schema.indexes.len();
        remove_index_defs(&mut schema, &index_name);
        if schema.indexes.len() == old_len {
            if if_exists {
                continue;
            }
            return Err(MongrelQueryError::Schema(format!(
                "index {index_name} does not exist on {table_name}"
            )));
        }
        rebuild_table(session, db, &table_name, schema, query)?;
    }
    session.clear_cache();
    Ok(())
}

fn insert_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    insert: Insert,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let table = match &insert.table {
        TableObject::TableName(name) => object_name(name)?,
        _ => {
            return Err(MongrelQueryError::Schema(
                "INSERT target must be a table name".into(),
            ));
        }
    };
    // SQL INSERT → require Insert permission on the target table.
    db.require_table(
        &table,
        mongreldb_core::auth_state::RequiredPermission::Insert,
    )?;
    if insert.returning.is_some() {
        return Err(MongrelQueryError::Schema(
            "INSERT RETURNING is not supported".into(),
        ));
    }
    if session.view_definition(&table).is_some() {
        return insert_view_rows(session, db, &table, insert, query);
    }
    if let Some(entry) = db.external_table(&table) {
        return insert_external_rows(session, db, &entry, insert, query);
    }
    let schema = table_schema(db, &table)?;
    let columns = insert_columns(&schema, &insert.columns)?;
    let rows = values_rows(insert.source.as_deref())?;
    let mut ops = PendingSqlOps::default();
    for (index, row) in rows.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        if row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "INSERT has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let mut cells = Vec::with_capacity(row.len());
        for (col, expr) in columns.iter().zip(row.iter()) {
            cells.push((col.id, expr_to_value(expr, col.ty.clone())?));
        }

        match &insert.on {
            Some(OnInsert::OnConflict(conflict)) => match &conflict.action {
                OnConflictAction::DoNothing => {
                    if pk_conflict(db, &table, &schema, &cells)? {
                        continue;
                    }
                    ops.push(PendingSqlOp::Put {
                        table: table.clone(),
                        cells,
                    })?;
                }
                OnConflictAction::DoUpdate(update) => {
                    if let Some(existing) = pk_conflict_row(db, &table, &schema, &cells)? {
                        let excluded = cells_to_map(&cells);
                        let mut merged = existing.columns.clone();
                        for assignment in &update.assignments {
                            apply_assignment(
                                session,
                                &schema,
                                &mut merged,
                                assignment,
                                Some(&excluded),
                                query,
                            )?;
                        }
                        ops.push(PendingSqlOp::Delete {
                            table: table.clone(),
                            row_id: existing.row_id,
                        })?;
                        ops.push(PendingSqlOp::Put {
                            table: table.clone(),
                            cells: map_to_cells(&merged),
                        })?;
                    } else {
                        ops.push(PendingSqlOp::Put {
                            table: table.clone(),
                            cells,
                        })?;
                    }
                }
            },
            Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
                if let Some(existing) = pk_conflict_row(db, &table, &schema, &cells)? {
                    let excluded = cells_to_map(&cells);
                    let mut merged = existing.columns.clone();
                    for assignment in assignments {
                        apply_assignment(
                            session,
                            &schema,
                            &mut merged,
                            assignment,
                            Some(&excluded),
                            query,
                        )?;
                    }
                    ops.push(PendingSqlOp::Delete {
                        table: table.clone(),
                        row_id: existing.row_id,
                    })?;
                    ops.push(PendingSqlOp::Put {
                        table: table.clone(),
                        cells: map_to_cells(&merged),
                    })?;
                } else {
                    ops.push(PendingSqlOp::Put {
                        table: table.clone(),
                        cells,
                    })?;
                }
            }
            None => {
                ops.push(PendingSqlOp::Put {
                    table: table.clone(),
                    cells,
                })?;
            }
            Some(_) => {
                return Err(MongrelQueryError::Schema(
                    "this INSERT conflict action is not supported".into(),
                ));
            }
        }
    }
    let changes = logical_changes_spooled(&mut ops, query)?;
    let last_insert_rowid = last_insert_pk_spooled(&mut ops, &schema, query)?;
    stage_or_apply_spooled(session, db, ops, changes, last_insert_rowid)
}

#[derive(Clone)]
struct SqlTriggerEventImage {
    kind: TriggerEvent,
    old: Option<HashMap<u16, Value>>,
    new: Option<HashMap<u16, Value>>,
}

fn insert_view_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    view: &str,
    insert: Insert,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if insert.on.is_some() {
        return Err(MongrelQueryError::Schema(
            "INSERT conflict actions are not supported for views".into(),
        ));
    }
    let view_def = session
        .view_definition(view)
        .ok_or_else(|| MongrelQueryError::Schema(format!("view {view:?} does not exist")))?;
    if view_def.schema.columns.is_empty() {
        return Err(MongrelQueryError::Schema(
            "INSERT into a view requires CREATE VIEW column names".into(),
        ));
    }
    let triggers = instead_of_triggers(db, view, TriggerEvent::Insert, None);
    if triggers.is_empty() {
        return Err(MongrelQueryError::Schema(format!(
            "cannot INSERT into view {view:?} without an INSTEAD OF INSERT trigger"
        )));
    }

    let columns = insert_columns(&view_def.schema, &insert.columns)?;
    let rows = values_rows(insert.source.as_deref())?;
    let mut ops = PendingSqlOps::default();
    for (index, row) in rows.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        if row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "INSERT has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let mut new = HashMap::new();
        for (col, expr) in columns.iter().zip(row.iter()) {
            let mut value = expr_to_untyped_value(expr)?;
            if let Some(ty) = view_def.input_types.get(&col.id).and_then(|ty| ty.clone()) {
                value = coerce_value(value, ty)?;
            }
            new.insert(col.id, value);
        }
        let event = SqlTriggerEventImage {
            kind: TriggerEvent::Insert,
            old: None,
            new: Some(new),
        };
        for trigger in &triggers {
            if execute_instead_of_trigger_program(session, db, trigger, &event, &mut ops, query)?
                == SqlTriggerProgramOutcome::Ignore
            {
                break;
            }
        }
    }
    let changes = logical_changes_spooled(&mut ops, query)?;
    stage_or_apply_spooled(session, db, ops, changes, None)
}

fn instead_of_triggers(
    db: &Arc<Database>,
    view: &str,
    event: TriggerEvent,
    changed_columns: Option<&[u16]>,
) -> Vec<StoredTrigger> {
    db.triggers()
        .into_iter()
        .filter(|trigger| {
            if !trigger.enabled
                || trigger.timing != TriggerTiming::InsteadOf
                || trigger.event != event
                || !matches!(&trigger.target, TriggerTarget::View(target) if target == view)
            {
                return false;
            }
            if event == TriggerEvent::Update && !trigger.update_of.is_empty() {
                let Some(changed_columns) = changed_columns else {
                    return false;
                };
                return trigger.update_of.iter().any(|name| {
                    trigger
                        .target_columns
                        .iter()
                        .find(|column| column.name == *name)
                        .is_some_and(|column| changed_columns.contains(&column.id))
                });
            }
            true
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlTriggerProgramOutcome {
    Continue,
    Ignore,
}

fn execute_instead_of_trigger_program(
    session: &MongrelSession,
    db: &Arc<Database>,
    trigger: &StoredTrigger,
    event: &SqlTriggerEventImage,
    ops: &mut PendingSqlOps,
    query: &RegisteredSqlQuery,
) -> Result<SqlTriggerProgramOutcome> {
    if let Some(when) = &trigger.when {
        if !eval_instead_of_trigger_expr(when, event)? {
            return Ok(SqlTriggerProgramOutcome::Continue);
        }
    }
    for (index, step) in trigger.program.steps.iter().enumerate() {
        command_checkpoint(session, query, index)?;
        match step {
            TriggerStep::SetNew { .. } => {
                return Err(MongrelQueryError::Schema(
                    "SET NEW is not valid in INSTEAD OF triggers".into(),
                ));
            }
            TriggerStep::Insert { table, cells } => {
                ops.push(PendingSqlOp::Put {
                    table: table.clone(),
                    cells: eval_instead_of_trigger_cells(cells, event)?,
                })?;
            }
            TriggerStep::UpdateByPk { table, pk, cells } => {
                let pk = eval_instead_of_trigger_value(pk, event)?;
                let handle = db.table(table)?;
                let row_id = handle.lock().lookup_pk(&pk.encode_key()).ok_or_else(|| {
                    MongrelQueryError::Schema(format!(
                        "trigger {:?} update target not found",
                        trigger.name
                    ))
                })?;
                let old = {
                    let guard = handle.lock();
                    guard.get(row_id, guard.snapshot()).ok_or_else(|| {
                        MongrelQueryError::Schema(format!(
                            "trigger {:?} update target not visible",
                            trigger.name
                        ))
                    })?
                };
                let mut merged = old.columns;
                for (column_id, value) in eval_instead_of_trigger_cells(cells, event)? {
                    merged.insert(column_id, value);
                }
                ops.push(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id,
                })?;
                ops.push(PendingSqlOp::Put {
                    table: table.clone(),
                    cells: map_to_cells(&merged),
                })?;
            }
            TriggerStep::DeleteByPk { table, pk } => {
                let pk = eval_instead_of_trigger_value(pk, event)?;
                let row_id = db
                    .table(table)?
                    .lock()
                    .lookup_pk(&pk.encode_key())
                    .ok_or_else(|| {
                        MongrelQueryError::Schema(format!(
                            "trigger {:?} delete target not found",
                            trigger.name
                        ))
                    })?;
                ops.push(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id,
                })?;
            }
            TriggerStep::Select { .. } => {}
            TriggerStep::Foreach { .. }
            | TriggerStep::DeleteWhere { .. }
            | TriggerStep::UpdateWhere { .. } => {
                return Err(MongrelQueryError::Schema(
                    "FOREACH/DELETE WHERE/UPDATE WHERE are not valid in INSTEAD OF triggers".into(),
                ));
            }
            TriggerStep::Raise { action, message } => match action {
                TriggerRaiseAction::Ignore => return Ok(SqlTriggerProgramOutcome::Ignore),
                TriggerRaiseAction::Abort
                | TriggerRaiseAction::Fail
                | TriggerRaiseAction::Rollback => {
                    let message = eval_instead_of_trigger_value(message, event)?;
                    return Err(MongrelQueryError::Schema(format!(
                        "trigger {:?} raised: {}",
                        trigger.name,
                        trigger_message(message)
                    )));
                }
            },
        }
    }
    Ok(SqlTriggerProgramOutcome::Continue)
}

fn eval_instead_of_trigger_cells(
    cells: &[TriggerCell],
    event: &SqlTriggerEventImage,
) -> Result<Vec<(u16, Value)>> {
    cells
        .iter()
        .map(|cell| {
            Ok((
                cell.column_id,
                eval_instead_of_trigger_value(&cell.value, event)?,
            ))
        })
        .collect()
}

fn eval_instead_of_trigger_expr(expr: &TriggerExpr, event: &SqlTriggerEventImage) -> Result<bool> {
    match expr {
        TriggerExpr::Value(value) => match eval_instead_of_trigger_value(value, event)? {
            Value::Bool(value) => Ok(value),
            Value::Null => Ok(false),
            other => Err(MongrelQueryError::Schema(format!(
                "trigger WHEN value must be boolean, got {other:?}"
            ))),
        },
        TriggerExpr::Eq { left, right } => Ok(trigger_values_equal(
            &eval_instead_of_trigger_value(left, event)?,
            &eval_instead_of_trigger_value(right, event)?,
        )),
        TriggerExpr::NotEq { left, right } => Ok(!trigger_values_equal(
            &eval_instead_of_trigger_value(left, event)?,
            &eval_instead_of_trigger_value(right, event)?,
        )),
        TriggerExpr::IsNull(value) => Ok(matches!(
            eval_instead_of_trigger_value(value, event)?,
            Value::Null
        )),
        TriggerExpr::IsNotNull(value) => Ok(!matches!(
            eval_instead_of_trigger_value(value, event)?,
            Value::Null
        )),
        TriggerExpr::Lt { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::Lt,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::Lte { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::LtEq,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::Gt { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::Gt,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::Gte { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::GtEq,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::And { left, right } => Ok(eval_instead_of_trigger_expr(left, event)?
            && eval_instead_of_trigger_expr(right, event)?),
        TriggerExpr::Or { left, right } => Ok(eval_instead_of_trigger_expr(left, event)?
            || eval_instead_of_trigger_expr(right, event)?),
        TriggerExpr::Not(inner) => Ok(!eval_instead_of_trigger_expr(inner, event)?),
    }
}

fn eval_instead_of_trigger_value(
    value: &TriggerValue,
    event: &SqlTriggerEventImage,
) -> Result<Value> {
    match value {
        TriggerValue::Literal(value) => Ok(value.clone()),
        TriggerValue::NewColumn(column_id) => {
            if event.kind == TriggerEvent::Delete {
                return Err(MongrelQueryError::Schema(
                    "DELETE triggers cannot reference NEW".into(),
                ));
            }
            event
                .new
                .as_ref()
                .and_then(|row| row.get(column_id))
                .cloned()
                .ok_or_else(|| MongrelQueryError::Schema("NEW column is not available".into()))
        }
        TriggerValue::OldColumn(column_id) => {
            if event.kind == TriggerEvent::Insert {
                return Err(MongrelQueryError::Schema(
                    "INSERT triggers cannot reference OLD".into(),
                ));
            }
            event
                .old
                .as_ref()
                .and_then(|row| row.get(column_id))
                .cloned()
                .ok_or_else(|| MongrelQueryError::Schema("OLD column is not available".into()))
        }
        TriggerValue::SelectedColumn(_) => Err(MongrelQueryError::Schema(
            "SELECTED column is not available in INSTEAD OF triggers".into(),
        )),
    }
}

/// Extract a `usize` from a SQL `Expr` (e.g. `Expr::Value(Number("5", false))`).
fn expr_to_usize(expr: &sqlparser::ast::Expr) -> Option<usize> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::Number(s, _) => s.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// Sort rows by ORDER BY expressions. Each `OrderByExpr` maps to a column
/// name; we resolve the name to a column id via the schema, then compare
/// the encoded key bytes for ordering.
fn apply_order_by(
    session: &MongrelSession,
    query: &RegisteredSqlQuery,
    rows: &mut [Row],
    order_by: &[sqlparser::ast::OrderByExpr],
    schema: &CoreSchema,
) -> Result<()> {
    let name_to_id: HashMap<String, u16> = schema
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.id))
        .collect();
    let sort_specs = order_by
        .iter()
        .filter_map(|expr| {
            let column_name = match &expr.expr {
                Expr::Identifier(ident) => Some(ident.value.as_str()),
                Expr::CompoundIdentifier(idents) => idents.last().map(|ident| ident.value.as_str()),
                _ => None,
            }?;
            name_to_id
                .get(column_name)
                .copied()
                .map(|column_id| (column_id, expr.options.asc == Some(false)))
        })
        .collect::<Vec<_>>();
    if rows.len() < 2 || sort_specs.is_empty() {
        return Ok(());
    }

    query.checkpoint()?;
    let key_entry_bytes = std::mem::size_of::<Vec<u8>>();
    let per_row_bytes = std::mem::size_of::<Vec<Vec<u8>>>()
        .saturating_add(sort_specs.len().saturating_mul(key_entry_bytes));
    let mut accounted_key_bytes = rows.len().saturating_mul(per_row_bytes);
    if accounted_key_bytes > ORDERED_DML_SORT_KEY_BYTES_LIMIT {
        return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
            resource: "ordered DML sort key bytes",
            requested: accounted_key_bytes,
            limit: ORDERED_DML_SORT_KEY_BYTES_LIMIT,
        }
        .into());
    }

    let mut keys = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.iter().enumerate() {
        command_checkpoint(session, query, row_index)?;
        let mut row_keys = Vec::with_capacity(sort_specs.len());
        for (column_id, _) in &sort_specs {
            // Older sparse rows can omit a declared column. Treat absence as
            // SQL NULL so comparison remains total and deterministic.
            let key = row
                .columns
                .get(column_id)
                .map_or_else(Vec::new, Value::encode_key);
            accounted_key_bytes = accounted_key_bytes.saturating_add(key.len());
            if accounted_key_bytes > ORDERED_DML_SORT_KEY_BYTES_LIMIT {
                return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "ordered DML sort key bytes",
                    requested: accounted_key_bytes,
                    limit: ORDERED_DML_SORT_KEY_BYTES_LIMIT,
                }
                .into());
            }
            row_keys.push(key);
        }
        keys.push(row_keys);
    }

    let compare = |left_index: usize, right_index: usize| {
        for (sort_index, (_, descending)) in sort_specs.iter().enumerate() {
            let ordering = keys[left_index][sort_index].cmp(&keys[right_index][sort_index]);
            let ordering = if *descending {
                ordering.reverse()
            } else {
                ordering
            };
            if !ordering.is_eq() {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    };

    let mut order = (0..rows.len()).collect::<Vec<_>>();
    for (run_index, run) in order.chunks_mut(ORDERED_DML_SORT_RUN_ROWS).enumerate() {
        command_checkpoint(
            session,
            query,
            run_index.saturating_mul(ORDERED_DML_SORT_RUN_ROWS),
        )?;
        run.sort_by(|left, right| compare(*left, *right));
    }

    let mut scratch = Vec::with_capacity(order.len());
    let mut run_width = ORDERED_DML_SORT_RUN_ROWS;
    while run_width < order.len() {
        scratch.clear();
        let mut run_start = 0_usize;
        while run_start < order.len() {
            let middle = run_start.saturating_add(run_width).min(order.len());
            let end = middle.saturating_add(run_width).min(order.len());
            let mut left = run_start;
            let mut right = middle;
            while left < middle || right < end {
                command_checkpoint(session, query, scratch.len())?;
                if right >= end || (left < middle && !compare(order[left], order[right]).is_gt()) {
                    scratch.push(order[left]);
                    left += 1;
                } else {
                    scratch.push(order[right]);
                    right += 1;
                }
            }
            run_start = end;
        }
        std::mem::swap(&mut order, &mut scratch);
        run_width = run_width.saturating_mul(2);
    }

    query.checkpoint()?;
    let mut destination = vec![0_usize; order.len()];
    for (new_index, old_index) in order.into_iter().enumerate() {
        if new_index % COMMAND_CHECKPOINT_ROWS == 0 {
            session.fire_test_hook(SqlTestHookPoint::DuringOrderedDmlPermutation);
            query.checkpoint()?;
        }
        destination[old_index] = new_index;
    }
    drop(keys);
    let mut permutation_steps = 0_usize;
    for index in 0..rows.len() {
        if permutation_steps.is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
            session.fire_test_hook(SqlTestHookPoint::DuringOrderedDmlPermutation);
            query.checkpoint()?;
        }
        permutation_steps = permutation_steps.saturating_add(1);
        while destination[index] != index {
            if permutation_steps.is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
                session.fire_test_hook(SqlTestHookPoint::DuringOrderedDmlPermutation);
                query.checkpoint()?;
            }
            let target = destination[index];
            rows.swap(index, target);
            destination.swap(index, target);
            permutation_steps = permutation_steps.saturating_add(1);
        }
    }
    Ok(())
}

async fn update_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    update: sqlparser::ast::Update,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if update.returning.is_some() || update.output.is_some() || update.from.is_some() {
        return Err(MongrelQueryError::Schema(
            "UPDATE RETURNING/OUTPUT/FROM are not supported".into(),
        ));
    }
    if !update.table.joins.is_empty() {
        return Err(MongrelQueryError::Schema(
            "UPDATE with joins is not supported".into(),
        ));
    }
    let table = table_factor_name(&update.table.relation)?;
    // SQL UPDATE → require Update permission on the target table (also
    // requires Select for the implicit read of the rows to update).
    db.require_table(
        &table,
        mongreldb_core::auth_state::RequiredPermission::Update,
    )?;
    db.require_table(
        &table,
        mongreldb_core::auth_state::RequiredPermission::Select,
    )?;
    if session.view_definition(&table).is_some() {
        return update_view_rows(session, db, &table, update, query).await;
    }
    if let Some(entry) = db.external_table(&table) {
        return update_external_rows(session, db, &entry, update, query);
    }
    // The matched-row scan below is a predicate read of the target table;
    // feed it into SSI certification when running serializable (S1B-002).
    session.record_serializable_table_read(&table);
    let limit = update.limit.as_ref().and_then(expr_to_usize);
    if update.order_by.is_empty() {
        let mut ops = PendingSqlOps::default();
        let mut changes = 0_u64;
        for_each_visible_row_controlled(session, db, &table, query, |schema, row| {
            if limit.is_some_and(|limit| changes >= limit as u64) {
                return Ok(());
            }
            if predicate_matches(update.selection.as_ref(), schema, &row, query)? {
                let mut merged = row.columns.clone();
                for assignment in &update.assignments {
                    apply_assignment(session, schema, &mut merged, assignment, None, query)?;
                }
                ops.push(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id: row.row_id,
                })?;
                ops.push(PendingSqlOp::Put {
                    table: table.clone(),
                    cells: map_to_cells(&merged),
                })?;
                changes = changes.saturating_add(1);
            }
            Ok(())
        })?;
        return stage_or_apply_spooled(session, db, ops, changes, None);
    }
    let mut matched = Vec::new();
    let mut matched_bytes = 0_usize;
    let schema = for_each_visible_row_controlled(session, db, &table, query, |schema, row| {
        if predicate_matches(update.selection.as_ref(), schema, &row, query)? {
            if matched.len() >= ORDERED_DML_MAX_MATCHED_ROWS {
                return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "ordered UPDATE matched rows",
                    requested: matched.len().saturating_add(1),
                    limit: ORDERED_DML_MAX_MATCHED_ROWS,
                }
                .into());
            }
            let requested_bytes = matched_bytes.saturating_add(row_deep_bytes(&row));
            if requested_bytes > ORDERED_DML_MATCHED_ROW_BYTES_LIMIT {
                return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "ordered UPDATE matched row bytes",
                    requested: requested_bytes,
                    limit: ORDERED_DML_MATCHED_ROW_BYTES_LIMIT,
                }
                .into());
            }
            matched_bytes = requested_bytes;
            matched.push(row);
        }
        Ok(())
    })?;
    // Apply ORDER BY + LIMIT if present.
    if !update.order_by.is_empty() {
        query.checkpoint()?;
        apply_order_by(session, query, &mut matched, &update.order_by, &schema)?;
        query.checkpoint()?;
    }
    if let Some(limit) = limit {
        matched.truncate(limit);
    }
    let mut ops = PendingSqlOps::default();
    for (index, row) in matched.iter().enumerate() {
        command_checkpoint(session, query, index)?;
        let mut merged = row.columns.clone();
        for assignment in &update.assignments {
            apply_assignment(session, &schema, &mut merged, assignment, None, query)?;
        }
        ops.push(PendingSqlOp::Delete {
            table: table.clone(),
            row_id: row.row_id,
        })?;
        ops.push(PendingSqlOp::Put {
            table: table.clone(),
            cells: map_to_cells(&merged),
        })?;
    }
    stage_or_apply_spooled(session, db, ops, matched.len() as u64, None)
}

async fn delete_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    delete: Delete,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if delete.returning.is_some()
        || delete.output.is_some()
        || delete.using.is_some()
        || !delete.tables.is_empty()
    {
        return Err(MongrelQueryError::Schema(
            "DELETE USING/RETURNING and multi-table DELETE are not supported".into(),
        ));
    }
    let table = single_from_table(&delete.from)?;
    // SQL DELETE → require Delete permission on the target table (also
    // requires Select for the implicit read of the rows to delete).
    db.require_table(
        &table,
        mongreldb_core::auth_state::RequiredPermission::Delete,
    )?;
    db.require_table(
        &table,
        mongreldb_core::auth_state::RequiredPermission::Select,
    )?;
    if session.view_definition(&table).is_some() {
        return delete_view_rows(session, db, &table, delete, query).await;
    }
    if let Some(entry) = db.external_table(&table) {
        return delete_external_rows(session, db, &entry, delete, query);
    }
    // The matched-row scan below is a predicate read of the target table;
    // feed it into SSI certification when running serializable (S1B-002).
    session.record_serializable_table_read(&table);
    let limit = delete.limit.as_ref().and_then(expr_to_usize);
    if delete.order_by.is_empty() {
        let mut ops = PendingSqlOps::default();
        let mut changes = 0_u64;
        for_each_visible_row_controlled(session, db, &table, query, |schema, row| {
            if limit.is_some_and(|limit| changes >= limit as u64) {
                return Ok(());
            }
            if predicate_matches(delete.selection.as_ref(), schema, &row, query)? {
                ops.push(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id: row.row_id,
                })?;
                changes = changes.saturating_add(1);
            }
            Ok(())
        })?;
        return stage_or_apply_spooled(session, db, ops, changes, None);
    }
    let mut matched = Vec::new();
    let mut matched_bytes = 0_usize;
    let schema = for_each_visible_row_controlled(session, db, &table, query, |schema, row| {
        if predicate_matches(delete.selection.as_ref(), schema, &row, query)? {
            if matched.len() >= ORDERED_DML_MAX_MATCHED_ROWS {
                return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "ordered DELETE matched rows",
                    requested: matched.len().saturating_add(1),
                    limit: ORDERED_DML_MAX_MATCHED_ROWS,
                }
                .into());
            }
            let requested_bytes = matched_bytes.saturating_add(row_deep_bytes(&row));
            if requested_bytes > ORDERED_DML_MATCHED_ROW_BYTES_LIMIT {
                return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "ordered DELETE matched row bytes",
                    requested: requested_bytes,
                    limit: ORDERED_DML_MATCHED_ROW_BYTES_LIMIT,
                }
                .into());
            }
            matched_bytes = requested_bytes;
            matched.push(row);
        }
        Ok(())
    })?;
    // Apply ORDER BY + LIMIT if present.
    if !delete.order_by.is_empty() {
        query.checkpoint()?;
        apply_order_by(session, query, &mut matched, &delete.order_by, &schema)?;
        query.checkpoint()?;
    }
    if let Some(limit) = limit {
        matched.truncate(limit);
    }
    let changes = matched.len() as u64;
    let mut ops = PendingSqlOps::default();
    for (index, row) in matched.into_iter().enumerate() {
        command_checkpoint(session, query, index)?;
        ops.push(PendingSqlOp::Delete {
            table: table.clone(),
            row_id: row.row_id,
        })?;
    }
    stage_or_apply_spooled(session, db, ops, changes, None)
}

async fn update_view_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    view: &str,
    update: sqlparser::ast::Update,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let view_def = session
        .view_definition(view)
        .ok_or_else(|| MongrelQueryError::Schema(format!("view {view:?} does not exist")))?;
    let changed_columns = view_assignment_targets(&view_def.schema, &update.assignments, query)?;
    let triggers = instead_of_triggers(db, view, TriggerEvent::Update, Some(&changed_columns));
    if triggers.is_empty() {
        return Err(MongrelQueryError::Schema(format!(
            "cannot UPDATE view {view:?} without a matching INSTEAD OF UPDATE trigger"
        )));
    }

    let mut ops = PendingSqlOps::default();
    for_each_materialized_view_row(session, &view_def, query, |old| {
        if view_row_matches(update.selection.as_ref(), &view_def.schema, &old, query)? {
            let mut new = old.clone();
            for assignment in &update.assignments {
                apply_view_assignment(
                    session,
                    &view_def.schema,
                    &view_def.input_types,
                    &mut new,
                    assignment,
                    query,
                )?;
            }
            let event = SqlTriggerEventImage {
                kind: TriggerEvent::Update,
                old: Some(old),
                new: Some(new),
            };
            for trigger in &triggers {
                if execute_instead_of_trigger_program(
                    session, db, trigger, &event, &mut ops, query,
                )? == SqlTriggerProgramOutcome::Ignore
                {
                    break;
                }
            }
        }
        Ok(())
    })
    .await?;
    let changes = logical_changes_spooled(&mut ops, query)?;
    stage_or_apply_spooled(session, db, ops, changes, None)
}

async fn delete_view_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    view: &str,
    delete: Delete,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let view_def = session
        .view_definition(view)
        .ok_or_else(|| MongrelQueryError::Schema(format!("view {view:?} does not exist")))?;
    let triggers = instead_of_triggers(db, view, TriggerEvent::Delete, None);
    if triggers.is_empty() {
        return Err(MongrelQueryError::Schema(format!(
            "cannot DELETE from view {view:?} without an INSTEAD OF DELETE trigger"
        )));
    }

    let mut ops = PendingSqlOps::default();
    for_each_materialized_view_row(session, &view_def, query, |old| {
        if view_row_matches(delete.selection.as_ref(), &view_def.schema, &old, query)? {
            let event = SqlTriggerEventImage {
                kind: TriggerEvent::Delete,
                old: Some(old),
                new: None,
            };
            for trigger in &triggers {
                if execute_instead_of_trigger_program(
                    session, db, trigger, &event, &mut ops, query,
                )? == SqlTriggerProgramOutcome::Ignore
                {
                    break;
                }
            }
        }
        Ok(())
    })
    .await?;
    let changes = logical_changes_spooled(&mut ops, query)?;
    stage_or_apply_spooled(session, db, ops, changes, None)
}

fn view_assignment_targets(
    schema: &CoreSchema,
    assignments: &[Assignment],
    query: &RegisteredSqlQuery,
) -> Result<Vec<u16>> {
    let mut out = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        query.checkpoint()?;
        let column_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name(name)?,
            AssignmentTarget::Tuple(_) => {
                return Err(MongrelQueryError::Schema(
                    "view UPDATE tuple assignments are not supported".into(),
                ));
            }
        };
        let column = schema
            .column(&column_name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
        out.push(column.id);
    }
    Ok(out)
}

fn apply_view_assignment(
    session: &MongrelSession,
    schema: &CoreSchema,
    input_types: &HashMap<u16, Option<TypeId>>,
    row: &mut HashMap<u16, Value>,
    assignment: &Assignment,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    session.fire_test_hook(SqlTestHookPoint::BeforeAssignmentEvaluation);
    query.checkpoint()?;
    let column_name = match &assignment.target {
        AssignmentTarget::ColumnName(name) => object_name(name)?,
        AssignmentTarget::Tuple(_) => {
            return Err(MongrelQueryError::Schema(
                "view UPDATE tuple assignments are not supported".into(),
            ));
        }
    };
    let column = schema
        .column(&column_name)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
    let mut value = eval_value_expr(&assignment.value, schema, row, None, query)?;
    if let Some(ty) = input_types.get(&column.id).and_then(|ty| ty.clone()) {
        value = coerce_value(value, ty)?;
    }
    row.insert(column.id, value);
    Ok(())
}

fn view_row_matches(
    selection: Option<&Expr>,
    schema: &CoreSchema,
    row: &HashMap<u16, Value>,
    query: &RegisteredSqlQuery,
) -> Result<bool> {
    match selection {
        Some(expr) => eval_bool_expr(expr, schema, row, query),
        None => Ok(true),
    }
}

async fn for_each_materialized_view_row<F>(
    session: &MongrelSession,
    view: &crate::ViewDef,
    query: &RegisteredSqlQuery,
    mut consume: F,
) -> Result<()>
where
    F: FnMut(HashMap<u16, Value>) -> Result<()> + Send,
{
    if view.schema.columns.is_empty() {
        return Err(MongrelQueryError::Schema(
            "view write routing requires CREATE VIEW column names".into(),
        ));
    }
    let sql = format!(
        "SELECT * FROM ({}) AS __mongreldb_view_materialize",
        view.sql
    );
    let sql = crate::rewrite_compat_function_calls(&session.resolve_view_sql(&sql));
    let df = session.dataframe_with_query(&sql, query).await?;
    let mut stream = session.execute_dataframe_stream(df, query).await?;

    let mut converted = 0_usize;
    while let Some(batch) = next_command_batch(&mut stream, query).await? {
        if batch.num_columns() != view.schema.columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "view query returned {} columns for {} declared view columns",
                batch.num_columns(),
                view.schema.columns.len()
            )));
        }
        for row_idx in 0..batch.num_rows() {
            command_checkpoint(session, query, converted)?;
            let mut view_row = HashMap::new();
            for (col_idx, view_col) in view.schema.columns.iter().enumerate() {
                let scalar = datafusion::common::ScalarValue::try_from_array(
                    batch.column(col_idx).as_ref(),
                    row_idx,
                )
                .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                let mut value = scalar_to_core_value(scalar)?;
                if let Some(ty) = view.input_types.get(&view_col.id).and_then(|ty| ty.clone()) {
                    value = coerce_value(value, ty)?;
                }
                view_row.insert(view_col.id, value);
            }
            consume(view_row)?;
            converted += 1;
        }
    }
    Ok(())
}

fn scalar_to_core_value(value: datafusion::common::ScalarValue) -> Result<Value> {
    use datafusion::common::ScalarValue;
    if value.is_null() {
        return Ok(Value::Null);
    }
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Value::Bool(v)),
        ScalarValue::Float16(Some(v)) => Ok(Value::Float64(f32::from(v) as f64)),
        ScalarValue::Float32(Some(v)) => Ok(Value::Float64(v as f64)),
        ScalarValue::Float64(Some(v)) => Ok(Value::Float64(v)),
        ScalarValue::Int8(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Int16(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Int32(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Int64(Some(v)) => Ok(Value::Int64(v)),
        ScalarValue::UInt8(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::UInt16(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::UInt32(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v)
            .map(Value::Int64)
            .map_err(|_| MongrelQueryError::Schema(format!("view value {v} exceeds i64 range"))),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Ok(Value::Bytes(v.into_bytes())),
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v))
        | ScalarValue::LargeBinary(Some(v)) => Ok(Value::Bytes(v)),
        ScalarValue::Date32(Some(v))
        | ScalarValue::Time32Second(Some(v))
        | ScalarValue::Time32Millisecond(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Date64(Some(v))
        | ScalarValue::Time64Microsecond(Some(v))
        | ScalarValue::Time64Nanosecond(Some(v))
        | ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _)
        | ScalarValue::DurationSecond(Some(v))
        | ScalarValue::DurationMillisecond(Some(v))
        | ScalarValue::DurationMicrosecond(Some(v))
        | ScalarValue::DurationNanosecond(Some(v)) => Ok(Value::Int64(v)),
        ScalarValue::Dictionary(_, value) | ScalarValue::RunEndEncoded(_, _, value) => {
            scalar_to_core_value(*value)
        }
        ScalarValue::Decimal128(Some(v), _, _) => Ok(Value::Decimal(v)),
        other => Err(MongrelQueryError::Schema(format!(
            "view write routing cannot materialize {other:?}"
        ))),
    }
}

fn truncate_tables(
    session: &MongrelSession,
    db: &Arc<Database>,
    truncate: Truncate,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let mut ops = Vec::new();
    for (table_index, target) in truncate.table_names.into_iter().enumerate() {
        command_checkpoint(session, query, table_index)?;
        let table = object_name(&target.name)?;
        // SQL TRUNCATE → require Delete permission on each target table.
        db.require_table(
            &table,
            mongreldb_core::auth_state::RequiredPermission::Delete,
        )?;
        if let Some(entry) = db.external_table(&table) {
            return Err(external_table_write_error("TRUNCATE", &entry));
        }
        if let Ok(handle) = db.table(&table) {
            let changes = handle.lock().count();
            ops.push(PendingSqlOp::Truncate { table, changes });
        } else if !truncate.if_exists {
            return Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::NotFound(format!("table {table:?} not found")),
            ));
        }
    }
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

fn stage_or_apply(
    session: &MongrelSession,
    db: &Arc<Database>,
    ops: Vec<PendingSqlOp>,
    changes: u64,
    last_insert_rowid: Option<u64>,
) -> Result<()> {
    let ops = PendingSqlOps::from_vec(ops)?;
    stage_or_apply_spooled(session, db, ops, changes, last_insert_rowid)
}

fn stage_or_apply_spooled(
    session: &MongrelSession,
    db: &Arc<Database>,
    mut ops: PendingSqlOps,
    changes: u64,
    last_insert_rowid: Option<u64>,
) -> Result<()> {
    if ops.is_empty() {
        session.sql_fn_state.record_changes(0, None);
        return Ok(());
    }
    let query = session.current_query()?;
    let mut transaction = session.transaction.lock();
    if let Some(staged) = transaction.staged_ops.as_mut() {
        staged.append_from(&mut ops, &query)?;
        drop(transaction);
        session.fire_test_hook(SqlTestHookPoint::AfterStatementStaging);
        session.current_query()?.checkpoint()?;
        session
            .sql_fn_state
            .record_changes(changes, last_insert_rowid);
        return Ok(());
    }
    drop(transaction);
    let external_tables = external_tables_to_refresh_spooled(db, &mut ops, &query)?;
    let epoch = match apply_ops(session, db, &mut ops, &query) {
        Ok(Some(epoch)) => epoch,
        Ok(None) => {
            return Err(MongrelQueryError::InvalidQueryState(
                "non-empty SQL write produced no commit epoch".into(),
            ));
        }
        Err(error) => {
            if matches!(
                &error,
                MongrelQueryError::CommitOutcome {
                    committed: true,
                    ..
                }
            ) {
                if let Err(refresh_error) = sync_committed_statement(
                    session,
                    db,
                    &external_tables,
                    changes,
                    last_insert_rowid,
                    &query,
                ) {
                    if matches!(
                        &refresh_error,
                        MongrelQueryError::QueryCancelled { .. }
                            | MongrelQueryError::DeadlineExceeded { .. }
                    ) {
                        return Err(refresh_error);
                    }
                    return Err(query.commit_outcome_error(format!(
                        "{error}; external table refresh failed: {refresh_error}"
                    )));
                }
            }
            return Err(error);
        }
    };
    query.record_commit_with_ts(
        query.status().statement_index,
        epoch.0,
        db.commit_ts_for_epoch(epoch),
    );
    if let Err(error) = query.exit_commit_critical() {
        return Err(query.commit_outcome_error(error.to_string()));
    }
    session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
    if let Err(error) = sync_committed_statement(
        session,
        db,
        &external_tables,
        changes,
        last_insert_rowid,
        &query,
    ) {
        if matches!(
            &error,
            MongrelQueryError::QueryCancelled { .. } | MongrelQueryError::DeadlineExceeded { .. }
        ) {
            return Err(error);
        }
        return Err(query.commit_outcome_error(error.to_string()));
    }
    query.checkpoint()?;
    Ok(())
}

fn external_tables_in_spooled_ops(
    ops: &mut PendingSqlOps,
    query: &RegisteredSqlQuery,
) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    query.checkpoint()?;
    for (index, op) in ops.reader()?.enumerate() {
        if index & 63 == 0 {
            query.checkpoint()?;
        }
        if let PendingSqlOp::ExternalState { table, .. } = op? {
            if seen.insert(table.clone()) {
                names.push(table);
            }
        }
    }
    query.checkpoint()?;
    Ok(names)
}

fn external_tables_to_refresh_spooled(
    db: &Arc<Database>,
    ops: &mut PendingSqlOps,
    query: &RegisteredSqlQuery,
) -> Result<Vec<String>> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for name in external_tables_in_spooled_ops(ops, query)? {
        if seen.insert(name.clone()) {
            names.push(name);
        }
    }
    for (index, entry) in db.external_tables().into_iter().enumerate() {
        if index & 63 == 0 {
            query.checkpoint()?;
        }
        if seen.insert(entry.name.clone()) {
            names.push(entry.name);
        }
    }
    query.checkpoint()?;
    Ok(names)
}

fn logical_changes_spooled(ops: &mut PendingSqlOps, query: &RegisteredSqlQuery) -> Result<u64> {
    let mut explicit = 0_u64;
    let mut puts = 0_u64;
    let mut deletes = 0_u64;
    query.checkpoint()?;
    for (index, op) in ops.reader()?.enumerate() {
        if index & 63 == 0 {
            query.checkpoint()?;
        }
        match op? {
            PendingSqlOp::ExternalState { changes, .. }
            | PendingSqlOp::Truncate { changes, .. } => {
                explicit = explicit.saturating_add(changes);
            }
            PendingSqlOp::Put { .. } => puts = puts.saturating_add(1),
            PendingSqlOp::Delete { .. } => deletes = deletes.saturating_add(1),
        }
    }
    query.checkpoint()?;
    Ok(explicit.saturating_add(if puts > 0 { puts } else { deletes }))
}

fn last_insert_pk_spooled(
    ops: &mut PendingSqlOps,
    schema: &CoreSchema,
    query: &RegisteredSqlQuery,
) -> Result<Option<u64>> {
    let Some(pk) = schema.primary_key() else {
        return Ok(None);
    };
    let mut last = None;
    let mut visited_cells = 0_usize;
    query.checkpoint()?;
    for (index, op) in ops.reader()?.enumerate() {
        if index & 63 == 0 {
            query.checkpoint()?;
        }
        if let PendingSqlOp::Put { cells, .. } = op? {
            for (column_id, value) in cells {
                if visited_cells & 255 == 0 {
                    query.checkpoint()?;
                }
                visited_cells = visited_cells.saturating_add(1);
                if column_id == pk.id {
                    if let Value::Int64(value) = value {
                        if value >= 0 {
                            last = Some(value as u64);
                        }
                    }
                    break;
                }
            }
        }
    }
    query.checkpoint()?;
    Ok(last)
}

fn refresh_external_tables(
    session: &MongrelSession,
    db: &Arc<Database>,
    names: &[String],
    query: &RegisteredSqlQuery,
) -> Result<()> {
    for name in names {
        if let Some(entry) = db.external_table(name) {
            match refresh_external_table_provider(session, db, &entry, Some(query)) {
                Ok(()) => {}
                Err(error)
                    if query.status().committed
                        && matches!(
                            error,
                            MongrelQueryError::QueryCancelled { .. }
                                | MongrelQueryError::DeadlineExceeded { .. }
                        ) =>
                {
                    // The durable publication already won. Finish required
                    // session repair without the cancelled control, then
                    // return the original typed cancellation outcome.
                    refresh_external_table_provider(session, db, &entry, None)?;
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

fn sync_committed_statement(
    session: &MongrelSession,
    db: &Arc<Database>,
    external_tables: &[String],
    changes: u64,
    last_insert_rowid: Option<u64>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let refresh = refresh_external_tables(session, db, external_tables, query);
    session
        .sql_fn_state
        .record_changes(changes, last_insert_rowid);
    session.clear_cache();
    refresh
}

fn logical_changes(ops: &[PendingSqlOp]) -> u64 {
    let explicit = ops
        .iter()
        .filter_map(|op| match op {
            PendingSqlOp::ExternalState { changes, .. } => Some(*changes),
            PendingSqlOp::Truncate { changes, .. } => Some(*changes),
            _ => None,
        })
        .sum::<u64>();
    let puts = ops
        .iter()
        .filter(|op| matches!(op, PendingSqlOp::Put { .. }))
        .count() as u64;
    let row_changes = if puts > 0 {
        puts
    } else {
        ops.iter()
            .filter(|op| matches!(op, PendingSqlOp::Delete { .. }))
            .count() as u64
    };
    explicit + row_changes
}

struct QueryExternalTriggerBridge {
    db: Arc<Database>,
    modules: Arc<ExternalModuleRegistry>,
    query: RegisteredSqlQuery,
}

impl ExternalTriggerBridge for QueryExternalTriggerBridge {
    fn apply_trigger_external_write(
        &self,
        entry: &ExternalTableEntry,
        base_state: Vec<u8>,
        op: ExternalTriggerWrite,
    ) -> mongreldb_core::Result<ExternalTriggerWriteResult> {
        let external_op = match op {
            ExternalTriggerWrite::Insert { cells, .. } => ExternalWriteOp::Insert {
                rows: vec![cells.into_iter().collect()],
            },
            ExternalTriggerWrite::UpdateByPk { pk, cells, .. } => {
                self.query.checkpoint().map_err(query_error_to_core)?;
                let mut rows = self
                    .modules
                    .external_table_rows_from_state(&self.db, entry, &base_state, &self.query)
                    .map_err(query_error_to_core)?;
                self.query.checkpoint().map_err(query_error_to_core)?;
                let pk_col = entry.declared_schema.primary_key().ok_or_else(|| {
                    mongreldb_core::MongrelError::InvalidArgument(format!(
                        "external trigger update target {:?} has no primary key",
                        entry.name
                    ))
                })?;
                let pk_key = pk.encode_key();
                let mut changed = 0_u64;
                for (row_index, row) in rows.iter_mut().enumerate() {
                    if row_index % COMMAND_CHECKPOINT_ROWS == 0 {
                        self.query.checkpoint().map_err(query_error_to_core)?;
                    }
                    if row
                        .get(&pk_col.id)
                        .is_some_and(|value| value.encode_key() == pk_key)
                    {
                        for (column_id, value) in &cells {
                            row.insert(*column_id, value.clone());
                        }
                        changed = changed.saturating_add(1);
                    }
                }
                if changed == 0 {
                    return Err(mongreldb_core::MongrelError::NotFound(format!(
                        "external trigger update target {:?} row not found",
                        entry.name
                    )));
                }
                ExternalWriteOp::ReplaceRows {
                    rows,
                    changes: changed,
                }
            }
            ExternalTriggerWrite::DeleteByPk { pk, .. } => {
                self.query.checkpoint().map_err(query_error_to_core)?;
                let rows = self
                    .modules
                    .external_table_rows_from_state(&self.db, entry, &base_state, &self.query)
                    .map_err(query_error_to_core)?;
                self.query.checkpoint().map_err(query_error_to_core)?;
                let pk_col = entry.declared_schema.primary_key().ok_or_else(|| {
                    mongreldb_core::MongrelError::InvalidArgument(format!(
                        "external trigger delete target {:?} has no primary key",
                        entry.name
                    ))
                })?;
                let pk_key = pk.encode_key();
                let before = rows.len();
                let mut kept = Vec::with_capacity(rows.len());
                for (row_index, row) in rows.into_iter().enumerate() {
                    if row_index % COMMAND_CHECKPOINT_ROWS == 0 {
                        self.query.checkpoint().map_err(query_error_to_core)?;
                    }
                    if row
                        .get(&pk_col.id)
                        .is_none_or(|value| value.encode_key() != pk_key)
                    {
                        kept.push(row);
                    }
                }
                let rows = kept;
                let changes = before.saturating_sub(rows.len()) as u64;
                if changes == 0 {
                    return Err(mongreldb_core::MongrelError::NotFound(format!(
                        "external trigger delete target {:?} row not found",
                        entry.name
                    )));
                }
                ExternalWriteOp::ReplaceRows { rows, changes }
            }
        };
        self.query.checkpoint().map_err(query_error_to_core)?;
        let (state, _result, base_writes) = self
            .modules
            .external_table_write(&self.db, entry, base_state, external_op, &self.query)
            .map_err(query_error_to_core)?;
        self.query.checkpoint().map_err(query_error_to_core)?;
        Ok(ExternalTriggerWriteResult {
            state,
            base_writes: base_writes
                .into_iter()
                .map(core_base_write_from_query)
                .collect(),
        })
    }
}

fn core_base_write_from_query(op: ExternalBaseWrite) -> ExternalTriggerBaseWrite {
    match op {
        ExternalBaseWrite::Put { table, cells } => ExternalTriggerBaseWrite::Put { table, cells },
        ExternalBaseWrite::Delete { table, row_id } => ExternalTriggerBaseWrite::Delete {
            table,
            row_id: RowId(row_id),
        },
    }
}

fn query_error_to_core(err: MongrelQueryError) -> mongreldb_core::MongrelError {
    match err {
        MongrelQueryError::Core(err) => err,
        MongrelQueryError::Schema(msg) => mongreldb_core::MongrelError::InvalidArgument(msg),
        MongrelQueryError::Arrow(msg) | MongrelQueryError::DataFusion(msg) => {
            mongreldb_core::MongrelError::Other(msg)
        }
        MongrelQueryError::QueryCancelled { .. } => mongreldb_core::MongrelError::Cancelled,
        MongrelQueryError::DeadlineExceeded { .. } => {
            mongreldb_core::MongrelError::DeadlineExceeded
        }
        other => mongreldb_core::MongrelError::Other(other.to_string()),
    }
}

fn apply_ops(
    session: &MongrelSession,
    db: &Arc<Database>,
    ops: &mut PendingSqlOps,
    query: &RegisteredSqlQuery,
) -> Result<Option<mongreldb_core::Epoch>> {
    if ops.is_empty() {
        return Ok(None);
    }
    let (isolation, predicate_reads) = session.commit_isolation_and_predicate_reads();
    let bridge = QueryExternalTriggerBridge {
        db: Arc::clone(db),
        modules: Arc::clone(&session.external_modules),
        query: query.clone(),
    };
    let mut tx = match isolation.canonical() {
        mongreldb_core::IsolationLevel::Serializable => {
            // SSI certification only runs when the commit carries
            // `Serializable`, but core's bridged constructors begin at the
            // default level, so a serializable SQL commit begins with
            // `begin_with_isolation` instead. That forgoes the external
            // trigger bridge — a trigger program writing an external table
            // then fails closed with TriggerValidation — and the session
            // principal override, so refuse to commit under a different
            // identity than the session's.
            if !session_principal_matches_database(session, db) {
                return Err(MongrelQueryError::InvalidQueryState(
                    "SERIALIZABLE SQL transactions require the session principal to be the database principal".into(),
                ));
            }
            db.begin_with_isolation(mongreldb_core::IsolationLevel::Serializable)
        }
        _ => db.begin_with_external_trigger_bridge_as(&bridge, session.principal()),
    };
    // S1B-002 SSI feed: register the tables this SQL transaction scanned so
    // core certification aborts on a phantom invalidation (table granularity,
    // matching core).
    for table in &predicate_reads {
        tx.track_predicate_read(table)?;
    }
    let mut ops = ops.reader()?.peekable();
    let mut op_index = 0_usize;
    while let Some(op) = ops.next() {
        command_checkpoint(session, query, op_index)?;
        op_index += 1;
        match op? {
            PendingSqlOp::Put { table, cells } => {
                tx.put(&table, cells)?;
            }
            PendingSqlOp::Delete { table, row_id } => {
                let paired_update = matches!(
                    ops.peek(),
                    Some(Ok(PendingSqlOp::Put { table: put_table, .. })) if put_table == &table
                );
                if paired_update {
                    let next = ops.next().ok_or_else(|| {
                        MongrelQueryError::InvalidQueryState(
                            "spooled update lost its paired put".into(),
                        )
                    })??;
                    let PendingSqlOp::Put { cells, .. } = next else {
                        return Err(MongrelQueryError::InvalidQueryState(
                            "spooled update pair changed after inspection".into(),
                        ));
                    };
                    tx.update_many(&table, vec![(row_id, cells)])?;
                } else {
                    tx.delete(&table, row_id)?;
                }
            }
            PendingSqlOp::ExternalState { table, state, .. } => {
                tx.put_external_state(&table, state)?;
            }
            PendingSqlOp::Truncate { table, .. } => {
                tx.truncate(&table)?;
            }
        }
    }
    query.checkpoint()?;
    session.fire_test_hook(SqlTestHookPoint::BeforeTransactionCommit);
    let mut fenced = false;
    let result = tx.commit_controlled(query.control(), || {
        enter_commit_fence(session, query).map_err(query_error_to_core)?;
        fenced = true;
        Ok(())
    });
    match result {
        Ok(epoch) => Ok(Some(epoch)),
        Err(mongreldb_core::MongrelError::DurableCommit { epoch, message }) => {
            query.record_commit(query.status().statement_index, epoch);
            if let Err(error) = query.exit_commit_critical() {
                return Err(query.commit_outcome_error(error.to_string()));
            }
            session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
            Err(query.commit_outcome_error(message))
        }
        Err(error) => {
            if fenced {
                let message = match query.exit_commit_critical() {
                    Ok(()) => error.to_string(),
                    Err(exit_error) => format!("{error}; {exit_error}"),
                };
                Err(query.outcome_unknown_error(message))
            } else {
                query.checkpoint()?;
                Err(error.into())
            }
        }
    }
}

fn schema_from_create_table(create: &CreateTable) -> Result<CoreSchema> {
    let mut primary_key: Option<String> = None;
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(pk) => {
                if pk.columns.len() != 1 {
                    return Err(MongrelQueryError::Schema(
                        "MongrelDB core supports a single-column primary key".into(),
                    ));
                }
                primary_key = Some(index_column_name(&pk.columns[0])?);
            }
            TableConstraint::Index(idx) => {
                let _ = idx;
            }
            TableConstraint::FulltextOrSpatial(idx) if idx.fulltext => {
                let _ = idx;
            }
            TableConstraint::Check(_) => {}
            TableConstraint::Unique(_)
            | TableConstraint::ForeignKey(_)
            | TableConstraint::PrimaryKeyUsingIndex(_)
            | TableConstraint::UniqueUsingIndex(_)
            | TableConstraint::FulltextOrSpatial(_) => {
                return Err(MongrelQueryError::Schema(
                    "UNIQUE, FOREIGN KEY, and SPATIAL constraints are not enforced by core SQL DDL; use MongrelDB Kit for those constraints".into(),
                ));
            }
        }
    }

    let mut columns = Vec::with_capacity(create.columns.len());
    for (i, col) in create.columns.iter().enumerate() {
        let is_table_pk = primary_key.as_deref() == Some(col.name.value.as_str());
        columns.push(core_column_from_sql((i + 1) as u16, col, is_table_pk)?);
    }

    let mut schema = CoreSchema {
        schema_id: 0,
        columns,
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    };
    for constraint in &create.constraints {
        match constraint {
            TableConstraint::Index(idx) => {
                let name = idx
                    .name
                    .as_ref()
                    .map(|n| n.value.clone())
                    .unwrap_or_else(|| "idx".into());
                add_index_defs(
                    &mut schema,
                    &name,
                    idx.columns.clone(),
                    index_kind_from_sql(idx.index_type.as_ref())?,
                )?;
            }
            TableConstraint::FulltextOrSpatial(idx) if idx.fulltext => {
                let name = idx
                    .opt_index_name
                    .as_ref()
                    .map(|n| n.value.clone())
                    .unwrap_or_else(|| "fulltext_idx".into());
                add_index_defs(&mut schema, &name, idx.columns.clone(), IndexKind::FmIndex)?;
            }
            TableConstraint::Check(check) => {
                if check.enforced == Some(false) {
                    return Err(MongrelQueryError::Schema(
                        "NOT ENFORCED CHECK constraints are not supported".into(),
                    ));
                }
                let expr = lower_check_expr(&check.expr, &schema)?;
                expr.validate()?;
                let id = (schema.constraints.checks.len() + 1) as u16;
                schema.constraints.checks.push(CoreCheckConstraint {
                    id,
                    name: check
                        .name
                        .as_ref()
                        .map(|name| name.value.clone())
                        .unwrap_or_else(|| format!("check_{id}")),
                    expr,
                });
            }
            _ => {}
        }
    }
    for column in &create.columns {
        for option in &column.options {
            if let ColumnOption::Check(check) = &option.option {
                if check.enforced == Some(false) {
                    return Err(MongrelQueryError::Schema(
                        "NOT ENFORCED CHECK constraints are not supported".into(),
                    ));
                }
                let expr = lower_check_expr(&check.expr, &schema)?;
                expr.validate()?;
                let id = (schema.constraints.checks.len() + 1) as u16;
                schema.constraints.checks.push(CoreCheckConstraint {
                    id,
                    name: check
                        .name
                        .as_ref()
                        .map(|name| name.value.clone())
                        .unwrap_or_else(|| format!("check_{}_{}", column.name.value, id)),
                    expr,
                });
            }
        }
    }
    schema.validate_auto_increment()?;
    Ok(schema)
}

fn lower_check_expr(expr: &Expr, schema: &CoreSchema) -> Result<CheckExpr> {
    let boxed = |expr| lower_check_expr(expr, schema).map(Box::new);
    match expr {
        Expr::Nested(expr) => lower_check_expr(expr, schema),
        Expr::Identifier(identifier) => schema
            .column(&identifier.value)
            .map(|column| CheckExpr::Col(column.id))
            .ok_or_else(|| {
                MongrelQueryError::Schema(format!(
                    "CHECK references unknown column {}",
                    identifier.value
                ))
            }),
        Expr::CompoundIdentifier(parts) => {
            let name = parts
                .last()
                .ok_or_else(|| MongrelQueryError::Schema("empty CHECK column reference".into()))?;
            schema
                .column(&name.value)
                .map(|column| CheckExpr::Col(column.id))
                .ok_or_else(|| {
                    MongrelQueryError::Schema(format!(
                        "CHECK references unknown column {}",
                        name.value
                    ))
                })
        }
        Expr::Value(value) => Ok(CheckExpr::Lit(sql_value_to_value(&value.value, None)?)),
        Expr::IsNull(expr) => Ok(CheckExpr::IsNull(check_column_id(expr, schema)?)),
        Expr::IsNotNull(expr) => Ok(CheckExpr::IsNotNull(check_column_id(expr, schema)?)),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(CheckExpr::Not(boxed(expr)?)),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => lower_check_expr(expr, schema),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => Ok(CheckExpr::Sub(
            Box::new(CheckExpr::Lit(Value::Int64(0))),
            boxed(expr)?,
        )),
        Expr::BinaryOp { left, op, right } => {
            let left_lowered = || boxed(left);
            let right_lowered = || boxed(right);
            match op {
                BinaryOperator::Eq => Ok(CheckExpr::Eq(left_lowered()?, right_lowered()?)),
                BinaryOperator::NotEq => Ok(CheckExpr::Ne(left_lowered()?, right_lowered()?)),
                BinaryOperator::Lt => Ok(CheckExpr::Lt(left_lowered()?, right_lowered()?)),
                BinaryOperator::LtEq => Ok(CheckExpr::Le(left_lowered()?, right_lowered()?)),
                BinaryOperator::Gt => Ok(CheckExpr::Gt(left_lowered()?, right_lowered()?)),
                BinaryOperator::GtEq => Ok(CheckExpr::Ge(left_lowered()?, right_lowered()?)),
                BinaryOperator::And => Ok(CheckExpr::And(left_lowered()?, right_lowered()?)),
                BinaryOperator::Or => Ok(CheckExpr::Or(left_lowered()?, right_lowered()?)),
                BinaryOperator::Plus => Ok(CheckExpr::Add(left_lowered()?, right_lowered()?)),
                BinaryOperator::Minus => Ok(CheckExpr::Sub(left_lowered()?, right_lowered()?)),
                BinaryOperator::Multiply => Ok(CheckExpr::Mul(left_lowered()?, right_lowered()?)),
                BinaryOperator::Divide => Ok(CheckExpr::Div(left_lowered()?, right_lowered()?)),
                BinaryOperator::Modulo => Ok(CheckExpr::Mod(left_lowered()?, right_lowered()?)),
                BinaryOperator::PGRegexMatch | BinaryOperator::Regexp => {
                    lower_regex_check(left, right, schema, false, false)
                }
                BinaryOperator::PGRegexIMatch => {
                    lower_regex_check(left, right, schema, false, true)
                }
                BinaryOperator::PGRegexNotMatch => {
                    lower_regex_check(left, right, schema, true, false)
                }
                BinaryOperator::PGRegexNotIMatch => {
                    lower_regex_check(left, right, schema, true, true)
                }
                BinaryOperator::Custom(operator)
                    if matches!(operator.as_str(), "~" | "~*" | "!~" | "!~*") =>
                {
                    lower_regex_check(
                        left,
                        right,
                        schema,
                        operator.starts_with('!'),
                        operator.ends_with('*'),
                    )
                }
                _ => Err(MongrelQueryError::Schema(format!(
                    "unsupported CHECK operator {op}"
                ))),
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let between = CheckExpr::And(
                Box::new(CheckExpr::Ge(boxed(expr)?, boxed(low)?)),
                Box::new(CheckExpr::Le(boxed(expr)?, boxed(high)?)),
            );
            Ok(if *negated {
                CheckExpr::Not(Box::new(between))
            } else {
                between
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let mut lowered = list
                .iter()
                .map(|value| Ok(CheckExpr::Eq(boxed(expr)?, boxed(value)?)));
            let first = lowered
                .next()
                .transpose()?
                .unwrap_or_else(|| CheckExpr::Not(Box::new(CheckExpr::True)));
            let any = lowered.try_fold(first, |left, right: Result<CheckExpr>| {
                Ok::<_, MongrelQueryError>(CheckExpr::Or(Box::new(left), Box::new(right?)))
            })?;
            Ok(if *negated {
                CheckExpr::Not(Box::new(any))
            } else {
                any
            })
        }
        Expr::Cast { expr, .. } => lower_check_expr(expr, schema),
        _ => Err(MongrelQueryError::Schema(format!(
            "unsupported CHECK expression: {expr}"
        ))),
    }
}

fn check_column_id(expr: &Expr, schema: &CoreSchema) -> Result<u16> {
    match lower_check_expr(expr, schema)? {
        CheckExpr::Col(column_id) => Ok(column_id),
        _ => Err(MongrelQueryError::Schema(
            "IS NULL in CHECK requires a column reference".into(),
        )),
    }
}

fn lower_regex_check(
    left: &Expr,
    right: &Expr,
    schema: &CoreSchema,
    negated: bool,
    case_insensitive: bool,
) -> Result<CheckExpr> {
    let col = check_column_id(left, schema)?;
    let pattern = match lower_check_expr(right, schema)? {
        CheckExpr::Lit(Value::Bytes(pattern)) => String::from_utf8(pattern).map_err(|error| {
            MongrelQueryError::Schema(format!("CHECK regex pattern is not UTF-8: {error}"))
        })?,
        _ => {
            return Err(MongrelQueryError::Schema(
                "CHECK regex requires a string literal pattern".into(),
            ))
        }
    };
    Ok(CheckExpr::Regex {
        col,
        pattern,
        negated,
        case_insensitive,
        cached: std::sync::OnceLock::new(),
    })
}

fn core_column_from_sql(
    id: u16,
    col: &ColumnDef,
    table_primary_key: bool,
) -> Result<CoreColumnDef> {
    let mut flags = ColumnFlags::empty().with(ColumnFlags::NULLABLE);
    let mut primary_key = table_primary_key;
    let mut auto_increment = false;
    let mut default_value: Option<DefaultExpr> = None;
    for opt in &col.options {
        match &opt.option {
            ColumnOption::NotNull => flags = flags.without(ColumnFlags::NULLABLE),
            ColumnOption::Null => flags = flags.with(ColumnFlags::NULLABLE),
            ColumnOption::PrimaryKey(_) => {
                primary_key = true;
                flags = flags.without(ColumnFlags::NULLABLE);
            }
            ColumnOption::DialectSpecific(tokens) => {
                let text = tokens
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(" ")
                    .to_ascii_lowercase();
                if text.contains("auto_increment") || text.contains("autoincrement") {
                    auto_increment = true;
                    flags = flags.without(ColumnFlags::NULLABLE);
                }
            }
            ColumnOption::Check(_) => {}
            ColumnOption::Unique(_) | ColumnOption::ForeignKey(_) => {
                return Err(MongrelQueryError::Schema(
                    "column UNIQUE and REFERENCES constraints are provided by MongrelDB Kit, not core SQL DDL".into(),
                ));
            }
            ColumnOption::Default(expr) => {
                // Accept literal defaults plus DEFAULT NOW() / DEFAULT UUID().
                default_value = Some(sql_expr_to_default(expr)?);
            }
            _ => {}
        }
    }
    if primary_key {
        flags = flags
            .with(ColumnFlags::PRIMARY_KEY)
            .without(ColumnFlags::NULLABLE);
    }
    if auto_increment {
        flags = flags.with(ColumnFlags::AUTO_INCREMENT);
    }
    Ok(CoreColumnDef {
        id,
        name: col.name.value.clone(),
        ty: sql_type_to_core(&col.data_type)?,
        flags,
        default_value,
        embedding_source: None,
    })
}

/// Convert a SQL `DEFAULT <expr>` into an engine [`DefaultExpr`]. Accepts:
/// - `DEFAULT TRUE` / `DEFAULT FALSE` / `DEFAULT NULL` / `DEFAULT <literal>`
/// - `DEFAULT NOW()` / `DEFAULT CURRENT_TIMESTAMP`
/// - `DEFAULT UUID()`
fn sql_expr_to_default(expr: &Expr) -> Result<DefaultExpr> {
    match expr {
        Expr::Function(f) => {
            let name = f.name.to_string().to_ascii_lowercase();
            match name.as_str() {
                "now" | "current_timestamp" => Ok(DefaultExpr::Now),
                "uuid" => Ok(DefaultExpr::Uuid),
                _ => Err(MongrelQueryError::Schema(format!(
                    "unsupported DEFAULT function: {name}"
                ))),
            }
        }
        other => {
            let v = expr_to_untyped_value(other)?;
            Ok(DefaultExpr::Static(v))
        }
    }
}

fn sql_type_to_core(data_type: &DataType) -> Result<TypeId> {
    // Handle ENUM specially — we need the variant list, which the string-based
    // approach below would discard.
    if let DataType::Enum(members, _) = data_type {
        use sqlparser::ast::EnumMember;
        let variants: Vec<String> = members
            .iter()
            .map(|m| match m {
                EnumMember::Name(n) => n.clone(),
                EnumMember::NamedValue(n, _) => n.clone(),
            })
            .collect();
        return Ok(TypeId::Enum {
            variants: Arc::from(variants),
        });
    }
    let text = data_type.to_string().to_ascii_lowercase();
    let base = text.split('(').next().unwrap_or(text.as_str()).trim();
    match base {
        "bigint" | "int8" | "int64" | "integer" | "int" | "int4" | "smallint" | "int2"
        | "tinyint" | "mediumint" => Ok(TypeId::Int64),
        "double" | "double precision" | "float8" | "float64" | "real" | "float" => {
            Ok(TypeId::Float64)
        }
        "varchar" | "character varying" | "char varying" | "text" | "string" | "bytes"
        | "bytea" | "blob" | "varbinary" | "binary" => Ok(TypeId::Bytes),
        "boolean" | "bool" => Ok(TypeId::Bool),
        "decimal" | "numeric" => Ok(TypeId::Decimal128 {
            precision: 38,
            scale: 2,
        }),
        "date" => Ok(TypeId::Date32),
        "time" => Ok(TypeId::Time64),
        "timestamp" | "datetime" => Ok(TypeId::TimestampNanos),
        "interval" => Ok(TypeId::Interval),
        "uuid" | "uniqueidentifier" => Ok(TypeId::Uuid),
        "json" | "jsonb" => Ok(TypeId::Json),
        "array" | "list" => Ok(TypeId::Array { element_type: 0 }),
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported column type: {other}"
        ))),
    }
}

fn insert_columns<'a>(
    schema: &'a CoreSchema,
    columns: &[ObjectName],
) -> Result<Vec<&'a CoreColumnDef>> {
    if columns.is_empty() {
        return Ok(schema.columns.iter().collect());
    }
    columns
        .iter()
        .map(|name| {
            let name = object_name(name)?;
            schema
                .column(&name)
                .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {name}")))
        })
        .collect()
}

fn values_rows(source: Option<&Query>) -> Result<Vec<Vec<Expr>>> {
    let Some(query) = source else {
        return Ok(vec![Vec::new()]);
    };
    match query.body.as_ref() {
        SetExpr::Values(values) => Ok(values.rows.iter().map(|row| row.to_vec()).collect()),
        _ => Err(MongrelQueryError::Schema(
            "INSERT currently supports VALUES rows, not INSERT ... SELECT".into(),
        )),
    }
}

fn apply_assignment(
    session: &MongrelSession,
    schema: &CoreSchema,
    row: &mut HashMap<u16, Value>,
    assignment: &Assignment,
    excluded: Option<&HashMap<u16, Value>>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    session.fire_test_hook(SqlTestHookPoint::BeforeAssignmentEvaluation);
    query.checkpoint()?;
    let column_name = match &assignment.target {
        AssignmentTarget::ColumnName(name) => object_name(name)?,
        AssignmentTarget::Tuple(_) => {
            return Err(MongrelQueryError::Schema(
                "tuple assignments are not supported".into(),
            ));
        }
    };
    let column = schema
        .column(&column_name)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
    let value = eval_value_expr(&assignment.value, schema, row, excluded, query)?;
    row.insert(column.id, coerce_value(value, column.ty.clone())?);
    Ok(())
}

fn predicate_matches(
    selection: Option<&Expr>,
    schema: &CoreSchema,
    row: &Row,
    query: &RegisteredSqlQuery,
) -> Result<bool> {
    match selection {
        Some(expr) => eval_bool_expr(expr, schema, &row.columns, query),
        None => Ok(true),
    }
}

fn eval_bool_expr(
    expr: &Expr,
    schema: &CoreSchema,
    row: &HashMap<u16, Value>,
    query: &RegisteredSqlQuery,
) -> Result<bool> {
    match expr {
        Expr::Nested(e) => eval_bool_expr(e, schema, row, query),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(!eval_bool_expr(expr, schema, row, query)?),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(eval_bool_expr(left, schema, row, query)?
                && eval_bool_expr(right, schema, row, query)?),
            BinaryOperator::Or => Ok(eval_bool_expr(left, schema, row, query)?
                || eval_bool_expr(right, schema, row, query)?),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let l = eval_value_expr(left, schema, row, None, query)?;
                let r = eval_value_expr(right, schema, row, None, query)?;
                compare_values(&l, op, &r)
            }
            _ => Err(MongrelQueryError::Schema(format!(
                "unsupported predicate operator: {op}"
            ))),
        },
        Expr::IsNull(e) => Ok(matches!(
            eval_value_expr(e, schema, row, None, query)?,
            Value::Null
        )),
        Expr::IsNotNull(e) => Ok(!matches!(
            eval_value_expr(e, schema, row, None, query)?,
            Value::Null
        )),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = eval_value_expr(expr, schema, row, None, query)?;
            let lo = eval_value_expr(low, schema, row, None, query)?;
            let hi = eval_value_expr(high, schema, row, None, query)?;
            let result = compare_values(&value, &BinaryOperator::GtEq, &lo)?
                && compare_values(&value, &BinaryOperator::LtEq, &hi)?;
            Ok(if *negated { !result } else { result })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_value_expr(expr, schema, row, None, query)?;
            let mut found = false;
            for (candidate_index, candidate) in list.iter().enumerate() {
                if candidate_index % COMMAND_CHECKPOINT_ROWS == 0 {
                    query.checkpoint()?;
                }
                if compare_values(
                    &value,
                    &BinaryOperator::Eq,
                    &eval_value_expr(candidate, schema, row, None, query)?,
                )? {
                    found = true;
                    break;
                }
            }
            Ok(if *negated { !found } else { found })
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let value = eval_value_expr(expr, schema, row, None, query)?;
            let pattern = eval_value_expr(pattern, schema, row, None, query)?;
            let (Value::Bytes(value), Value::Bytes(pattern)) = (value, pattern) else {
                return Ok(false);
            };
            let value = String::from_utf8_lossy(&value);
            let pattern = String::from_utf8_lossy(&pattern);
            let result = like_match(&value, &pattern, query)?;
            Ok(if *negated { !result } else { result })
        }
        Expr::Value(v) => match &v.value {
            SqlValue::Boolean(b) => Ok(*b),
            _ => Err(MongrelQueryError::Schema(
                "predicate literal must be boolean".into(),
            )),
        },
        _ => Err(MongrelQueryError::Schema(format!(
            "unsupported predicate expression: {expr}"
        ))),
    }
}

fn eval_value_expr(
    expr: &Expr,
    schema: &CoreSchema,
    row: &HashMap<u16, Value>,
    excluded: Option<&HashMap<u16, Value>>,
    query: &RegisteredSqlQuery,
) -> Result<Value> {
    query.checkpoint()?;
    match expr {
        Expr::Nested(e) => eval_value_expr(e, schema, row, excluded, query),
        Expr::Value(v) => sql_value_to_value(&v.value, None),
        Expr::Function(function) => ai_constructor_value(function),
        Expr::Identifier(ident) => {
            let col = schema.column(&ident.value).ok_or_else(|| {
                MongrelQueryError::Schema(format!("unknown column {}", ident.value))
            })?;
            Ok(row.get(&col.id).cloned().unwrap_or(Value::Null))
        }
        Expr::CompoundIdentifier(parts)
            if parts.len() == 2 && parts[0].value.eq_ignore_ascii_case("excluded") =>
        {
            let col = schema.column(&parts[1].value).ok_or_else(|| {
                MongrelQueryError::Schema(format!("unknown column {}", parts[1].value))
            })?;
            Ok(excluded
                .and_then(|values| values.get(&col.id).cloned())
                .unwrap_or(Value::Null))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match eval_value_expr(expr, schema, row, excluded, query)? {
            Value::Int64(v) => Ok(Value::Int64(v.saturating_neg())),
            Value::Float64(v) => Ok(Value::Float64(-v)),
            _ => Err(MongrelQueryError::Schema(
                "unary minus requires a numeric expression".into(),
            )),
        },
        _ => Err(MongrelQueryError::Schema(format!(
            "unsupported value expression: {expr}"
        ))),
    }
}

fn sql_value_to_value(value: &SqlValue, target: Option<TypeId>) -> Result<Value> {
    match value {
        SqlValue::Null => Ok(Value::Null),
        SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
        SqlValue::Number(raw, _) => {
            let raw = raw.to_string();
            if matches!(target, Some(TypeId::Float64))
                || raw.contains('.')
                || raw.contains('e')
                || raw.contains('E')
            {
                raw.parse::<f64>().map(Value::Float64).map_err(|e| {
                    MongrelQueryError::Schema(format!("invalid float literal {raw}: {e}"))
                })
            } else {
                raw.parse::<i64>().map(Value::Int64).map_err(|e| {
                    MongrelQueryError::Schema(format!("invalid integer literal {raw}: {e}"))
                })
            }
        }
        SqlValue::SingleQuotedString(s)
        | SqlValue::DoubleQuotedString(s)
        | SqlValue::EscapedStringLiteral(s)
        | SqlValue::UnicodeStringLiteral(s)
        | SqlValue::NationalStringLiteral(s)
        | SqlValue::TripleSingleQuotedString(s)
        | SqlValue::TripleDoubleQuotedString(s)
        | SqlValue::SingleQuotedByteStringLiteral(s)
        | SqlValue::DoubleQuotedByteStringLiteral(s)
        | SqlValue::TripleSingleQuotedByteStringLiteral(s)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(s)
        | SqlValue::SingleQuotedRawStringLiteral(s)
        | SqlValue::DoubleQuotedRawStringLiteral(s)
        | SqlValue::TripleSingleQuotedRawStringLiteral(s)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(s)
        | SqlValue::HexStringLiteral(s) => Ok(Value::Bytes(s.as_bytes().to_vec())),
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported SQL literal: {other}"
        ))),
    }
}

fn expr_to_value(expr: &Expr, ty: TypeId) -> Result<Value> {
    let value = match expr {
        Expr::Value(v) => sql_value_to_value(&v.value, Some(ty.clone()))?,
        Expr::Function(function) => ai_constructor_value(function)?,
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr_to_value(expr, ty.clone())? {
            Value::Int64(v) => Value::Int64(v.saturating_neg()),
            Value::Float64(v) => Value::Float64(-v),
            _ => {
                return Err(MongrelQueryError::Schema(
                    "unary minus requires a numeric literal".into(),
                ));
            }
        },
        _ => {
            return Err(MongrelQueryError::Schema(format!(
                "INSERT values must be literals, got {expr}"
            )));
        }
    };
    coerce_value(value, ty)
}

fn ai_constructor_value(function: &sqlparser::ast::Function) -> Result<Value> {
    let name = function.name.to_string().to_ascii_lowercase();
    if !matches!(name.as_str(), "mongreldb_sparse_vector" | "mongreldb_set") {
        return Err(MongrelQueryError::Schema(format!(
            "unsupported value function: {name}"
        )));
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return Err(MongrelQueryError::Schema(format!(
            "{name} requires one JSON string argument"
        )));
    };
    if arguments.args.len() != 1 {
        return Err(MongrelQueryError::Schema(format!(
            "{name} requires one JSON string argument"
        )));
    }
    let Value::Bytes(json) = expr_to_untyped_value(function_arg_expr(&arguments.args[0])?)? else {
        return Err(MongrelQueryError::Schema(format!(
            "{name} requires one JSON string argument"
        )));
    };
    if name == "mongreldb_sparse_vector" {
        let terms: Vec<(u32, f32)> = serde_json::from_slice(&json).map_err(|error| {
            MongrelQueryError::Schema(format!("invalid sparse vector JSON: {error}"))
        })?;
        if terms.is_empty() || terms.iter().any(|(_, weight)| !weight.is_finite()) {
            return Err(MongrelQueryError::Schema(
                "sparse vector must be non-empty with finite weights".into(),
            ));
        }
        let mut canonical = std::collections::BTreeMap::new();
        for (token, weight) in terms {
            let total = canonical.entry(token).or_insert(0.0f32);
            *total += weight;
            if !total.is_finite() {
                return Err(MongrelQueryError::Schema(
                    "sparse vector weights overflowed".into(),
                ));
            }
        }
        return mongreldb_core::query::encode_sparse_vector(
            &canonical.into_iter().collect::<Vec<_>>(),
        )
        .map(Value::Bytes)
        .map_err(MongrelQueryError::Core);
    }
    let members: Vec<serde_json::Value> = serde_json::from_slice(&json)
        .map_err(|error| MongrelQueryError::Schema(format!("invalid set JSON: {error}")))?;
    if members.iter().any(|member| {
        !matches!(
            member,
            serde_json::Value::String(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Bool(_)
        )
    }) {
        return Err(MongrelQueryError::Schema(
            "set members must be strings, numbers, or booleans".into(),
        ));
    }
    let canonical: std::collections::BTreeSet<_> = members
        .into_iter()
        .map(|member| serde_json::to_string(&member))
        .collect::<std::result::Result<_, _>>()
        .map_err(|error| MongrelQueryError::Schema(error.to_string()))?;
    Ok(Value::Bytes(
        format!("[{}]", canonical.into_iter().collect::<Vec<_>>().join(",")).into_bytes(),
    ))
}

fn expr_to_untyped_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => sql_value_to_value(&v.value, None),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr_to_untyped_value(expr)? {
            Value::Int64(v) => Ok(Value::Int64(v.saturating_neg())),
            Value::Float64(v) => Ok(Value::Float64(-v)),
            _ => Err(MongrelQueryError::Schema(
                "unary minus requires a numeric literal".into(),
            )),
        },
        _ => Err(MongrelQueryError::Schema(format!(
            "INSERT values must be literals, got {expr}"
        ))),
    }
}

fn coerce_value(value: Value, ty: TypeId) -> Result<Value> {
    match (value, ty) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Bool(v), TypeId::Bool) => Ok(Value::Bool(v)),
        (
            Value::Int64(v),
            TypeId::Int64
            | TypeId::Int32
            | TypeId::Int16
            | TypeId::Int8
            | TypeId::TimestampNanos
            | TypeId::Date32,
        ) => Ok(Value::Int64(v)),
        (Value::Float64(v), TypeId::Float64 | TypeId::Float32) => Ok(Value::Float64(v)),
        (Value::Int64(v), TypeId::Float64 | TypeId::Float32) => Ok(Value::Float64(v as f64)),
        (Value::Bytes(v), TypeId::Bytes) => Ok(Value::Bytes(v)),
        (Value::Bytes(v), TypeId::Enum { variants }) => {
            if variants.iter().any(|x| x.as_bytes() == &v[..]) {
                Ok(Value::Bytes(v))
            } else {
                let s = String::from_utf8_lossy(&v);
                Err(MongrelQueryError::Schema(format!(
                    "'{s}' is not a valid ENUM variant (allowed: {variants:?})"
                )))
            }
        }
        (Value::Bytes(v), TypeId::Embedding { .. }) => {
            let text =
                String::from_utf8(v).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
            let parsed: Vec<f32> = serde_json::from_str(&text)
                .map_err(|e| MongrelQueryError::Schema(format!("invalid embedding JSON: {e}")))?;
            Ok(Value::Embedding(parsed))
        }
        (v, ty) => Err(MongrelQueryError::Schema(format!(
            "value {v:?} cannot be stored in {ty:?}"
        ))),
    }
}

fn trigger_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int64(a), Value::Int64(b)) => a == b,
        (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Embedding(a), Value::Embedding(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(a, b)| a.to_bits() == b.to_bits())
        }
        _ => false,
    }
}

fn trigger_message(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Bytes(value) => String::from_utf8_lossy(&value).into_owned(),
        Value::Embedding(value) => format!("{value:?}"),
        Value::Decimal(value) => value.to_string(),
        Value::Interval {
            months,
            days,
            nanos,
        } => format!("{months} months {days} days {nanos} nanos"),
        Value::Uuid(b) => b.iter().map(|x| format!("{x:02x}")).collect(),
        Value::Json(b) => String::from_utf8_lossy(&b).into_owned(),
    }
}

fn compare_values(left: &Value, op: &BinaryOperator, right: &Value) -> Result<bool> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }
    let ordering = match (left, right) {
        (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
        (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
        (Value::Int64(a), Value::Float64(b)) => (*a as f64).partial_cmp(b),
        (Value::Float64(a), Value::Int64(b)) => a.partial_cmp(&(*b as f64)),
        (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
        (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
        (Value::Json(a), Value::Json(b)) => a.partial_cmp(b),
        _ => None,
    };
    let Some(ordering) = ordering else {
        return Ok(false);
    };
    Ok(match op {
        BinaryOperator::Eq => ordering.is_eq(),
        BinaryOperator::NotEq => !ordering.is_eq(),
        BinaryOperator::Gt => ordering.is_gt(),
        BinaryOperator::GtEq => ordering.is_gt() || ordering.is_eq(),
        BinaryOperator::Lt => ordering.is_lt(),
        BinaryOperator::LtEq => ordering.is_lt() || ordering.is_eq(),
        _ => {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported comparison operator {op}"
            )))
        }
    })
}

fn like_match(value: &str, pattern: &str, query: &RegisteredSqlQuery) -> Result<bool> {
    const MAX_LIKE_MATCH_STEPS: usize = 10_000_000;

    let value = value.chars().collect::<Vec<_>>();
    let pattern = pattern.chars().collect::<Vec<_>>();
    let mut value_index = 0_usize;
    let mut pattern_index = 0_usize;
    let mut star_index = None;
    let mut retry_value_index = 0_usize;
    let mut steps = 0_usize;

    while value_index < value.len() {
        if steps.is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
            query.checkpoint()?;
        }
        steps = steps.saturating_add(1);
        if steps > MAX_LIKE_MATCH_STEPS {
            return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "SQL LIKE matcher steps",
                requested: steps,
                limit: MAX_LIKE_MATCH_STEPS,
            }
            .into());
        }
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '_' || pattern[pattern_index] == value[value_index])
        {
            value_index += 1;
            pattern_index += 1;
        } else if pattern_index < pattern.len() && pattern[pattern_index] == '%' {
            star_index = Some(pattern_index);
            pattern_index += 1;
            retry_value_index = value_index;
        } else if let Some(star) = star_index {
            retry_value_index += 1;
            value_index = retry_value_index;
            pattern_index = star + 1;
        } else {
            return Ok(false);
        }
    }
    while pattern_index < pattern.len() && pattern[pattern_index] == '%' {
        pattern_index += 1;
    }
    Ok(pattern_index == pattern.len())
}

fn for_each_visible_row_controlled(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    query: &RegisteredSqlQuery,
    consume: impl FnMut(&CoreSchema, Row) -> Result<()>,
) -> Result<CoreSchema> {
    for_each_visible_row_at_snapshot_controlled(session, db, table, None, query, consume)
}

fn for_each_visible_row_at_snapshot_controlled(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    snapshot: Option<mongreldb_core::Snapshot>,
    query: &RegisteredSqlQuery,
    mut consume: impl FnMut(&CoreSchema, Row) -> Result<()>,
) -> Result<CoreSchema> {
    let handle = db.table(table)?;
    let guard = handle.lock();
    let schema = guard.schema().clone();
    let snapshot = snapshot.unwrap_or_else(|| guard.snapshot());
    let mut consumed = 0_usize;
    let mut callback_error = None;
    let result = guard.for_each_visible_row_controlled(snapshot, query.control(), |row| {
        if let Err(error) = command_checkpoint(session, query, consumed) {
            callback_error = Some(error);
            return Err(mongreldb_core::MongrelError::Cancelled);
        }
        consumed += 1;
        match consume(&schema, row) {
            Ok(()) => Ok(()),
            Err(error) => {
                callback_error = Some(error);
                Err(mongreldb_core::MongrelError::Other(
                    "visible-row callback failed".into(),
                ))
            }
        }
    });
    drop(guard);
    if let Some(error) = callback_error {
        return Err(error);
    }
    if let Err(error) = result {
        query.checkpoint()?;
        return Err(error.into());
    }
    Ok(schema)
}

fn table_schema(db: &Arc<Database>, table: &str) -> Result<CoreSchema> {
    match db.table(table) {
        Ok(handle) => {
            let schema = {
                let guard = handle.lock();
                guard.schema().clone()
            };
            Ok(schema)
        }
        Err(e) if matches!(e, mongreldb_core::MongrelError::NotFound(_)) => db
            .external_table(table)
            .map(|entry| entry.declared_schema)
            .ok_or_else(|| e.into()),
        Err(e) => Err(e.into()),
    }
}

fn rebuild_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    new_schema: CoreSchema,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    query.checkpoint()?;
    let saved_security = db.security_catalog();
    let has_saved_permissions = db
        .roles()
        .into_iter()
        .flat_map(|role| role.permissions)
        .any(|permission| permission_targets_table(&permission, table));
    if saved_security.table_has_objects(table) || has_saved_permissions {
        db.require_for(
            session.principal().as_ref(),
            &mongreldb_core::Permission::Admin,
        )?;
        validate_rebuilt_security(table, &new_schema, &saved_security)?;
    }
    let ttl = {
        let handle = db.table(table)?;
        let guard = handle.lock();
        guard.ttl().and_then(|policy| {
            new_schema
                .columns
                .iter()
                .find(|column| column.id == policy.column_id)
                .map(|column| (column.name.clone(), policy.duration_nanos))
        })
    };
    let temp = format!(
        "__mongreldb_ctas_build_rebuild_{}_{}",
        query.id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    db.create_rebuilding_table(&temp, table, &query.id().to_string(), new_schema.clone())?;
    if let Some((column, duration_nanos)) = &ttl {
        if let Err(error) = db.set_building_table_ttl(&temp, column, *duration_nanos) {
            let _ = db.discard_building_table(&temp);
            return Err(error.into());
        }
    }
    let mut chunk = Vec::with_capacity(COMMAND_CHECKPOINT_ROWS);
    let mut chunk_bytes = 0_usize;
    let copy_result = for_each_visible_row_controlled(session, db, table, query, |_schema, row| {
        let cells = row_to_schema_cells(&row, &new_schema);
        let row_bytes = cells_deep_bytes(&cells);
        if row_bytes > REBUILD_STAGING_BYTES_LIMIT {
            return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "table rebuild staged row bytes",
                requested: row_bytes,
                limit: REBUILD_STAGING_BYTES_LIMIT,
            }
            .into());
        }
        if !chunk.is_empty() && chunk_bytes.saturating_add(row_bytes) > REBUILD_STAGING_BYTES_LIMIT
        {
            commit_rebuild_chunk(session, db, &temp, std::mem::take(&mut chunk), query)?;
            chunk_bytes = 0;
        }
        chunk.push(cells);
        chunk_bytes = chunk_bytes.saturating_add(row_bytes);
        if chunk.len() == COMMAND_CHECKPOINT_ROWS {
            commit_rebuild_chunk(session, db, &temp, std::mem::take(&mut chunk), query)?;
            chunk_bytes = 0;
        }
        Ok(())
    })
    .and_then(|_| commit_rebuild_chunk(session, db, &temp, chunk, query));
    if let Err(error) = copy_result {
        let _ = db.discard_building_table(&temp);
        return Err(error);
    }
    if let Err(error) = query.checkpoint() {
        let _ = db.discard_building_table(&temp);
        return Err(error);
    }
    let fenced = std::cell::Cell::new(false);
    let publish = db.publish_rebuilding_table_controlled(&temp, table, || {
        enter_commit_fence(session, query).map_err(query_error_to_core)?;
        fenced.set(true);
        Ok(())
    });
    let (publish_epoch, publish_error) = match publish {
        Ok(epoch) => (epoch, None),
        Err(mongreldb_core::MongrelError::DurableCommit { epoch, message }) => {
            (mongreldb_core::Epoch(epoch), Some(message))
        }
        Err(error) if fenced.get() => {
            let _ = session.ctx.deregister_table(table);
            session.tables.lock().remove(table);
            let error = match register_table(session, db, table) {
                Ok(()) => error,
                Err(register_error) => mongreldb_core::MongrelError::Other(format!(
                    "{error}; table registration failed: {register_error}"
                )),
            };
            return Err(uncertain_fenced_error(session, query, error));
        }
        Err(error) => {
            query.checkpoint()?;
            let _ = db.discard_building_table(&temp);
            return Err(error.into());
        }
    };
    query.record_commit(query.status().statement_index, publish_epoch.0);
    let _ = session.ctx.deregister_table(table);
    session.tables.lock().remove(table);
    if let Err(error) = register_table(session, db, table) {
        if let Err(exit_error) = query.exit_commit_critical() {
            return Err(query.commit_outcome_error(format!("{error}; {exit_error}")));
        }
        session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
        return Err(query.commit_outcome_error(error.to_string()));
    }
    if let Err(error) = query.exit_commit_critical() {
        return Err(query.commit_outcome_error(error.to_string()));
    }
    session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
    if let Some(error) = publish_error {
        return Err(query.commit_outcome_error(error));
    }
    Ok(())
}

fn commit_rebuild_chunk(
    session: &MongrelSession,
    db: &Arc<Database>,
    temp: &str,
    rows: Vec<Vec<(u16, Value)>>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut transaction = db.begin_as(session.principal());
    for cells in rows {
        if let Err(error) = transaction.put_building(temp, cells) {
            transaction.rollback();
            return Err(error.into());
        }
    }
    transaction.commit_controlled(query.control(), || Ok(()))?;
    Ok(())
}

fn permission_targets_table(permission: &mongreldb_core::Permission, table: &str) -> bool {
    use mongreldb_core::Permission;
    matches!(
        permission,
        Permission::Select { table: target }
            | Permission::Insert { table: target }
            | Permission::Update { table: target }
            | Permission::Delete { table: target }
            | Permission::SelectColumns { table: target, .. }
            | Permission::InsertColumns { table: target, .. }
            | Permission::UpdateColumns { table: target, .. }
            if target == table
    )
}

fn validate_rebuilt_security(
    table: &str,
    schema: &CoreSchema,
    security: &mongreldb_core::SecurityCatalog,
) -> Result<()> {
    fn validate_expr(expr: &mongreldb_core::SecurityExpr, schema: &CoreSchema) -> Result<()> {
        match expr {
            mongreldb_core::SecurityExpr::True => Ok(()),
            mongreldb_core::SecurityExpr::ColumnEqCurrentUser { column }
            | mongreldb_core::SecurityExpr::ColumnEqValue { column, .. } => {
                if schema
                    .columns
                    .iter()
                    .any(|candidate| candidate.id == *column)
                {
                    Ok(())
                } else {
                    Err(MongrelQueryError::Schema(format!(
                        "cannot drop column {column}: row policy depends on it"
                    )))
                }
            }
            mongreldb_core::SecurityExpr::And { left, right }
            | mongreldb_core::SecurityExpr::Or { left, right } => {
                validate_expr(left, schema)?;
                validate_expr(right, schema)
            }
            mongreldb_core::SecurityExpr::Not { expression } => validate_expr(expression, schema),
        }
    }

    for policy in security
        .policies
        .iter()
        .filter(|policy| policy.table == table)
    {
        if let Some(expression) = &policy.using {
            validate_expr(expression, schema)?;
        }
        if let Some(expression) = &policy.with_check {
            validate_expr(expression, schema)?;
        }
    }
    for mask in security.masks.iter().filter(|mask| mask.table == table) {
        if !schema.columns.iter().any(|column| column.id == mask.column) {
            return Err(MongrelQueryError::Schema(format!(
                "cannot drop column {}: mask {} depends on it",
                mask.column, mask.name
            )));
        }
    }
    Ok(())
}

fn row_to_schema_cells(row: &Row, schema: &CoreSchema) -> Vec<(u16, Value)> {
    schema
        .columns
        .iter()
        .map(|col| {
            (
                col.id,
                row.columns.get(&col.id).cloned().unwrap_or(Value::Null),
            )
        })
        .collect()
}

fn current_column_flags(db: &Arc<Database>, table: &str, column: &str) -> Result<ColumnFlags> {
    let handle = db.table(table)?;
    let guard = handle.lock();
    guard
        .schema()
        .column(column)
        .map(|c| c.flags)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column}")))
}

fn register_table(session: &MongrelSession, db: &Arc<Database>, name: &str) -> Result<()> {
    let handle = db.table(name)?;
    let provider = MongrelProvider::new_secured(
        handle.clone(),
        Arc::clone(db),
        name.to_string(),
        session.principal(),
    )?;
    session
        .ctx
        .register_table(name, Arc::new(provider))
        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
    session.tables.lock().insert(name.to_string(), handle);
    Ok(())
}

fn add_index_defs(
    schema: &mut CoreSchema,
    name: &str,
    columns: Vec<IndexColumn>,
    kind: IndexKind,
) -> Result<()> {
    if columns.is_empty() {
        return Err(MongrelQueryError::Schema(
            "index must contain at least one column".into(),
        ));
    }
    for (i, index_col) in columns.iter().enumerate() {
        let col_name = index_column_name(index_col)?;
        let col = schema
            .column(&col_name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("unknown index column {col_name}")))?;
        let idx_name = if i == 0 {
            name.to_string()
        } else {
            format!("{name}_{col_name}")
        };
        schema.indexes.push(IndexDef {
            name: idx_name,
            column_id: col.id,
            kind,
            predicate: None,
            options: Default::default(),
        });
    }
    Ok(())
}

fn remove_index_defs(schema: &mut CoreSchema, name: &str) {
    let prefix = format!("{name}_");
    schema
        .indexes
        .retain(|idx| idx.name != name && !idx.name.starts_with(&prefix));
}

fn find_index_table(db: &Arc<Database>, index: &str) -> Result<Option<String>> {
    let mut found = None;
    for table in db.table_names() {
        let schema = table_schema(db, &table)?;
        if schema
            .indexes
            .iter()
            .any(|idx| idx.name == index || idx.name.starts_with(&format!("{index}_")))
        {
            if found.is_some() {
                return Ok(None);
            }
            found = Some(table);
        }
    }
    Ok(found)
}

fn index_kind_from_sql(using: Option<&sqlparser::ast::IndexType>) -> Result<IndexKind> {
    let Some(using) = using else {
        return Ok(IndexKind::Bitmap);
    };
    match using.to_string().to_ascii_lowercase().as_str() {
        "hash" | "btree" | "bitmap" => Ok(IndexKind::Bitmap),
        "gin" | "fulltext" | "fm" | "fm_index" => Ok(IndexKind::FmIndex),
        "brin" | "learned_range" | "range" => Ok(IndexKind::LearnedRange),
        "ann" | "hnsw" => Ok(IndexKind::Ann),
        "sparse" => Ok(IndexKind::Sparse),
        "minhash" | "lsh" => Ok(IndexKind::MinHash),
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported index type: {other}"
        ))),
    }
}

fn parse_index_options(kind: IndexKind, expressions: &[Expr]) -> Result<IndexOptions> {
    let mut options = IndexOptions::default();
    for expression in expressions {
        let Expr::BinaryOp { left, op, right } = expression else {
            return Err(MongrelQueryError::Schema(
                "index WITH options must use name = integer".into(),
            ));
        };
        if *op != BinaryOperator::Eq {
            return Err(MongrelQueryError::Schema(
                "index WITH options must use name = integer".into(),
            ));
        }
        let Expr::Identifier(name) = left.as_ref() else {
            return Err(MongrelQueryError::Schema(
                "index option name must be an identifier".into(),
            ));
        };
        let option_name = name.value.to_ascii_lowercase();
        if kind == IndexKind::Ann && option_name == "quantization" {
            let quantization = match right.as_ref() {
                Expr::Identifier(value) => value.value.clone(),
                Expr::Value(value) => match &value.value {
                    SqlValue::SingleQuotedString(value) | SqlValue::DoubleQuotedString(value) => {
                        value.clone()
                    }
                    _ => String::new(),
                },
                _ => String::new(),
            };
            if quantization != "binary_sign" {
                return Err(MongrelQueryError::Schema(
                    "ANN quantization must be 'binary_sign'".into(),
                ));
            }
            options
                .ann
                .get_or_insert_with(AnnOptions::default)
                .quantization = AnnQuantization::BinarySign;
            continue;
        }
        let value = expr_to_usize(right).ok_or_else(|| {
            MongrelQueryError::Schema(format!("index option {} must be an integer", name.value))
        })?;
        match (kind, option_name.as_str()) {
            (IndexKind::Ann, "m") => options.ann.get_or_insert_with(AnnOptions::default).m = value,
            (IndexKind::Ann, "ef_construction") => {
                options
                    .ann
                    .get_or_insert_with(AnnOptions::default)
                    .ef_construction = value
            }
            (IndexKind::Ann, "ef_search") => {
                options
                    .ann
                    .get_or_insert_with(AnnOptions::default)
                    .ef_search = value
            }
            (IndexKind::MinHash, "permutations") => {
                options
                    .minhash
                    .get_or_insert_with(MinHashOptions::default)
                    .permutations = value
            }
            (IndexKind::MinHash, "bands") => {
                options
                    .minhash
                    .get_or_insert_with(MinHashOptions::default)
                    .bands = value
            }
            (IndexKind::LearnedRange, "epsilon") => {
                options
                    .learned_range
                    .get_or_insert_with(LearnedRangeOptions::default)
                    .epsilon = value
            }
            (_, option) => {
                return Err(MongrelQueryError::Schema(format!(
                    "unsupported {kind:?} index option: {option}"
                )))
            }
        }
    }
    Ok(options)
}

fn index_column_name(index_col: &IndexColumn) -> Result<String> {
    match &index_col.column.expr {
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        other => Err(MongrelQueryError::Schema(format!(
            "index expressions are not supported, got {other}"
        ))),
    }
}

fn pk_conflict(
    db: &Arc<Database>,
    table: &str,
    schema: &CoreSchema,
    cells: &[(u16, Value)],
) -> Result<bool> {
    Ok(pk_conflict_row(db, table, schema, cells)?.is_some())
}

fn pk_conflict_row(
    db: &Arc<Database>,
    table: &str,
    schema: &CoreSchema,
    cells: &[(u16, Value)],
) -> Result<Option<Row>> {
    let Some(pk) = schema.primary_key() else {
        return Ok(None);
    };
    let Some((_, value)) = cells.iter().find(|(id, _)| *id == pk.id) else {
        return Ok(None);
    };
    if matches!(value, Value::Null) {
        return Ok(None);
    }
    let handle = db.table(table)?;
    let mut guard = handle.lock();
    // A deferred bulk load leaves HOT unbuilt; complete it before the point
    // lookup (Phase 14.7 lazy contract).
    guard.ensure_indexes_complete()?;
    let Some(row_id) = guard.lookup_pk(&value.encode_key()) else {
        return Ok(None);
    };
    let snapshot = guard.snapshot();
    Ok(guard.get(row_id, snapshot))
}

fn cells_to_map(cells: &[(u16, Value)]) -> HashMap<u16, Value> {
    cells
        .iter()
        .map(|(id, value)| (*id, value.clone()))
        .collect()
}

fn map_to_cells(row: &HashMap<u16, Value>) -> Vec<(u16, Value)> {
    row.iter().map(|(id, value)| (*id, value.clone())).collect()
}

fn single_from_table(from: &FromTable) -> Result<String> {
    let tables = match from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
    };
    if tables.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "DELETE supports exactly one table".into(),
        ));
    }
    table_with_joins_name(&tables[0])
}

fn table_with_joins_name(table: &TableWithJoins) -> Result<String> {
    if !table.joins.is_empty() {
        return Err(MongrelQueryError::Schema(
            "joins are not supported in this statement".into(),
        ));
    }
    table_factor_name(&table.relation)
}

fn table_factor_name(factor: &TableFactor) -> Result<String> {
    match factor {
        TableFactor::Table { name, .. } => object_name(name),
        _ => Err(MongrelQueryError::Schema("expected a table name".into())),
    }
}

fn object_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(MongrelQueryError::Schema(format!(
            "only unqualified object names are supported: {name}"
        )));
    }
    name.0[0]
        .as_ident()
        .map(|ident| ident.value.clone())
        .ok_or_else(|| MongrelQueryError::Schema(format!("invalid object name: {name}")))
}

fn strip_identifier(value: &str) -> Result<&str> {
    let value = value.trim().trim_end_matches(';').trim();
    if value.is_empty() {
        return Err(MongrelQueryError::Schema("missing identifier".into()));
    }
    Ok(value.trim_matches('"').trim_matches('`').trim_matches('\''))
}

fn parse_procedure_json<'a>(sql: &'a str, lower: &str, prefix: &str) -> Result<(&'a str, &'a str)> {
    let body = sql[prefix.len()..].trim();
    let lower_body = &lower[prefix.len()..];
    let marker = " as json ";
    let idx = lower_body
        .find(marker)
        .ok_or_else(|| MongrelQueryError::Schema("expected AS JSON '<procedure>'".into()))?;
    let name = strip_identifier(&body[..idx])?;
    let json = unquote_sql_string(&body[idx + marker.len()..])?;
    Ok((name, json))
}

fn parse_call_json<'a>(sql: &'a str, lower: &'a str) -> Result<(&'a str, HashMap<String, Value>)> {
    let body = sql[5..].trim().trim_end_matches(';').trim();
    let lower_body = lower[5..].trim().trim_end_matches(';').trim();
    let marker = "(json ";
    let idx = lower_body
        .find(marker)
        .ok_or_else(|| MongrelQueryError::Schema("expected CALL name(JSON '<args>')".into()))?;
    let name = strip_identifier(&body[..idx])?;
    let rest = body[idx + marker.len()..].trim();
    let rest = rest
        .strip_suffix(')')
        .ok_or_else(|| MongrelQueryError::Schema("expected closing ')' for CALL".into()))?
        .trim();
    let json = unquote_sql_string(rest)?;
    let raw: serde_json::Value =
        serde_json::from_str(json).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    let obj = raw
        .as_object()
        .ok_or_else(|| MongrelQueryError::Schema("CALL args JSON must be an object".into()))?;
    let mut args = HashMap::new();
    for (key, value) in obj {
        args.insert(key.clone(), json_value_to_core(value)?);
    }
    Ok((name, args))
}

fn unquote_sql_string(value: &str) -> Result<&str> {
    let value = value.trim().trim_end_matches(';').trim();
    if value.len() < 2 || !value.starts_with('\'') || !value.ends_with('\'') {
        return Err(MongrelQueryError::Schema(
            "expected single-quoted JSON".into(),
        ));
    }
    Ok(&value[1..value.len() - 1])
}

fn procedure_from_json(name: &str, json: &str) -> Result<StoredProcedure> {
    let parsed: StoredProcedure =
        serde_json::from_str(json).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    StoredProcedure::new(name, parsed.mode, parsed.params, parsed.body, 0)
        .map_err(MongrelQueryError::from)
}

fn json_value_to_core(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float64(value))
            } else {
                Err(MongrelQueryError::Schema("unsupported JSON number".into()))
            }
        }
        serde_json::Value::String(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(
            MongrelQueryError::Schema("procedure args only support scalar JSON values".into()),
        ),
    }
}

fn procedure_output_json(output: &ProcedureCallOutput) -> serde_json::Value {
    match output {
        ProcedureCallOutput::Null => serde_json::Value::Null,
        ProcedureCallOutput::Scalar(value) => core_value_json(value),
        ProcedureCallOutput::Row(row) => procedure_row_json(row),
        ProcedureCallOutput::Rows(rows) => {
            serde_json::Value::Array(rows.iter().map(procedure_row_json).collect())
        }
        ProcedureCallOutput::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), procedure_output_json(value)))
                .collect(),
        ),
        ProcedureCallOutput::Array(values) => {
            serde_json::Value::Array(values.iter().map(procedure_output_json).collect())
        }
    }
}

fn procedure_row_json(row: &ProcedureCallRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    if let Some(row_id) = row.row_id {
        obj.insert(
            "row_id".into(),
            serde_json::Value::String(row_id.0.to_string()),
        );
    }
    let columns = row
        .columns
        .iter()
        .map(|(id, value)| (id.to_string(), core_value_json(value)))
        .collect();
    obj.insert("columns".into(), serde_json::Value::Object(columns));
    serde_json::Value::Object(obj)
}

fn core_value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int64(value) => serde_json::Value::from(*value),
        Value::Float64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bytes(value) => String::from_utf8(value.clone())
            .map(serde_json::Value::String)
            .unwrap_or_else(|_| {
                serde_json::Value::Array(
                    value.iter().map(|b| serde_json::Value::from(*b)).collect(),
                )
            }),
        Value::Embedding(value) => serde_json::Value::Array(
            value
                .iter()
                .map(|v| {
                    serde_json::Number::from_f64(*v as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::Decimal(value) => serde_json::Value::String(value.to_string()),
        Value::Interval {
            months,
            days,
            nanos,
        } => serde_json::json!({"months": months, "days": days, "nanos": nanos}),
        Value::Uuid(b) => {
            let hex: String = b.iter().map(|x| format!("{x:02x}")).collect();
            serde_json::Value::String(hex)
        }
        Value::Json(b) => serde_json::from_slice(b)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(b).into_owned())),
    }
}

#[derive(Clone, Copy)]
enum TableMaintenance {
    Analyze,
    Compact,
}

fn run_table_maintenance(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    query: &RegisteredSqlQuery,
    operation: TableMaintenance,
) -> Result<bool> {
    query.checkpoint()?;
    let handle = db.table(table)?;
    let mut guard = handle.lock();
    let mut fence_error = None;
    let mut fenced = false;
    let result = match operation {
        TableMaintenance::Analyze => {
            guard.ensure_indexes_complete_controlled_with_receipt(query.control(), || {
                match enter_commit_fence(session, query) {
                    Ok(()) => {
                        fenced = true;
                        true
                    }
                    Err(error) => {
                        fence_error = Some(error);
                        false
                    }
                }
            })
        }
        TableMaintenance::Compact => guard.compact_controlled_with_receipt(query.control(), || {
            match enter_commit_fence(session, query) {
                Ok(()) => {
                    fenced = true;
                    true
                }
                Err(error) => {
                    fence_error = Some(error);
                    false
                }
            }
        }),
    };
    drop(guard);
    if let Some(error) = fence_error {
        return Err(error);
    }
    let (changed, receipt) = match result {
        Ok(result) => result,
        Err(error) => {
            if fenced {
                return Err(uncertain_fenced_error(session, query, error));
            } else {
                query.checkpoint()?;
            }
            return Err(error.into());
        }
    };
    if fenced {
        let Some(receipt) = receipt else {
            return Err(uncertain_fenced_error(
                session,
                query,
                mongreldb_core::MongrelError::Other(
                    "table maintenance published without a receipt".into(),
                ),
            ));
        };
        query.record_commit(query.status().statement_index, receipt.epoch.0);
        if let Err(error) = query.exit_commit_critical() {
            return Err(query.commit_outcome_error(error.to_string()));
        }
        session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
    }
    Ok(changed)
}

fn run_gc(session: &MongrelSession, db: &Arc<Database>, query: &RegisteredSqlQuery) -> Result<()> {
    query.checkpoint()?;
    let mut fence_error = None;
    let mut fenced = false;
    let result = db.gc_controlled_with_receipt(query.control(), || {
        match enter_commit_fence(session, query) {
            Ok(()) => {
                fenced = true;
                true
            }
            Err(error) => {
                fence_error = Some(error);
                false
            }
        }
    });
    if let Some(error) = fence_error {
        return Err(error);
    }
    let (reclaimed, receipt) = match result {
        Ok(result) => result,
        Err(error) => {
            if fenced {
                return Err(uncertain_fenced_error(session, query, error));
            } else {
                query.checkpoint()?;
            }
            return Err(error.into());
        }
    };
    if fenced {
        let Some(receipt) = receipt else {
            return Err(uncertain_fenced_error(
                session,
                query,
                mongreldb_core::MongrelError::Other(
                    "garbage collection published without a maintenance receipt".into(),
                ),
            ));
        };
        query.record_commit(query.status().statement_index, receipt.epoch.0);
        if let Err(error) = query.exit_commit_critical() {
            return Err(query.commit_outcome_error(error.to_string()));
        }
        session.fire_test_hook(SqlTestHookPoint::AfterDurableCommit);
    }
    let _ = reclaimed;
    Ok(())
}

fn compact_all(
    session: &MongrelSession,
    db: &Arc<Database>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    for (table_index, table) in db.table_names().into_iter().enumerate() {
        command_checkpoint(session, query, table_index)?;
        run_table_maintenance(session, db, &table, query, TableMaintenance::Compact)?;
    }
    run_gc(session, db, query)
}

fn analyze_all(
    session: &MongrelSession,
    db: &Arc<Database>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    for (table_index, table) in db.table_names().into_iter().enumerate() {
        command_checkpoint(session, query, table_index)?;
        run_table_maintenance(session, db, &table, query, TableMaintenance::Analyze)?;
    }
    Ok(())
}

fn reindex(
    session: &MongrelSession,
    db: &Arc<Database>,
    target: Option<&str>,
    query: &RegisteredSqlQuery,
) -> Result<()> {
    let tables = match target {
        None => db.table_names(),
        Some(name) if db.table_id(name).is_ok() => vec![name.to_string()],
        Some(name) => match find_index_table(db, name)? {
            Some(table) => vec![table],
            None => {
                return Err(MongrelQueryError::Schema(format!(
                    "REINDEX target {name:?} is not a table or index"
                )))
            }
        },
    };
    for (table_index, table) in tables.into_iter().enumerate() {
        command_checkpoint(session, query, table_index)?;
        run_table_maintenance(session, db, &table, query, TableMaintenance::Analyze)?;
        run_table_maintenance(session, db, &table, query, TableMaintenance::Compact)?;
    }
    run_gc(session, db, query)
}

fn parse_vacuum_into<'a>(sql: &'a str, lower: &str) -> Result<&'a str> {
    let marker = "vacuum into ";
    let body = sql[marker.len()..].trim().trim_end_matches(';').trim();
    let lower_body = lower[marker.len()..].trim().trim_end_matches(';').trim();
    if lower_body.is_empty() {
        return Err(MongrelQueryError::Schema(
            "VACUUM INTO requires a target directory path".into(),
        ));
    }
    if body.starts_with('\'') {
        unquote_sql_string(body)
    } else {
        strip_identifier(body)
    }
}

fn run_pragma(
    session: &MongrelSession,
    db: &Arc<Database>,
    sql: &str,
    lower: &str,
    query: &RegisteredSqlQuery,
) -> Result<RecordBatch> {
    let body = sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .get(6..)
        .ok_or_else(|| MongrelQueryError::Schema("expected PRAGMA name".into()))?
        .trim();
    let lower_body = lower
        .trim()
        .trim_end_matches(';')
        .trim()
        .get(6..)
        .ok_or_else(|| MongrelQueryError::Schema("expected PRAGMA name".into()))?
        .trim();
    let (name, arg) = parse_pragma_body(body, lower_body)?;
    match name.as_str() {
        "table_info" => pragma_table_info(db, required_pragma_arg(&name, arg.as_deref())?),
        "table_xinfo" => pragma_table_xinfo(db, required_pragma_arg(&name, arg.as_deref())?),
        "table_list" => pragma_table_list(session, db, arg.as_deref()),
        "index_list" => pragma_index_list(
            session,
            db,
            required_pragma_arg(&name, arg.as_deref())?,
            query,
        ),
        "index_info" => pragma_index_info(
            session,
            db,
            required_pragma_arg(&name, arg.as_deref())?,
            query,
        ),
        "index_xinfo" => pragma_index_xinfo(
            session,
            db,
            required_pragma_arg(&name, arg.as_deref())?,
            query,
        ),
        "foreign_key_list" => {
            pragma_foreign_key_list(db, required_pragma_arg(&name, arg.as_deref())?)
        }
        "foreign_key_check" => pragma_foreign_key_check(session, db, arg.as_deref(), query),
        "database_list" => pragma_database_list(db),
        "function_list" => pragma_function_list(),
        "module_list" => pragma_module_list(session),
        "trigger_list" => pragma_trigger_list(db),
        "collation_list" => pragma_collation_list(),
        "compile_options" => pragma_compile_options(),
        "integrity_check" => pragma_check_batch(db, query, "integrity_check"),
        "quick_check" => pragma_check_batch(db, query, "quick_check"),
        "schema_version" => int_batch("schema_version", schema_version(db)),
        "user_version" => {
            if let Some(value) = parse_optional_i64(arg.as_deref())? {
                if db.sql_pragma_i64("user_version")? != Some(value) {
                    run_controlled_durable_with_optional_epoch(session, query, |before_commit| {
                        let epoch = db.set_sql_pragma_i64_with_epoch_controlled(
                            "user_version",
                            value,
                            before_commit,
                        )?;
                        Ok(((), epoch.map(|epoch| epoch.0)))
                    })?;
                }
            }
            int_batch("user_version", get_db_pragma_i64(db, "user_version")?)
        }
        "application_id" => {
            if let Some(value) = parse_optional_i64(arg.as_deref())? {
                if db.sql_pragma_i64("application_id")? != Some(value) {
                    run_controlled_durable_with_optional_epoch(session, query, |before_commit| {
                        let epoch = db.set_sql_pragma_i64_with_epoch_controlled(
                            "application_id",
                            value,
                            before_commit,
                        )?;
                        Ok(((), epoch.map(|epoch| epoch.0)))
                    })?;
                }
            }
            int_batch("application_id", get_db_pragma_i64(db, "application_id")?)
        }
        "data_version" => int_batch("data_version", db.visible_epoch().0 as i64),
        "foreign_keys" => int_batch("foreign_keys", 1),
        "query_only" => int_batch("query_only", 0),
        "journal_mode" => strings_batch("journal_mode", vec!["wal".to_string()]),
        "synchronous" => int_batch("synchronous", 1),
        "encoding" => strings_batch("encoding", vec!["UTF-8".to_string()]),
        "page_size" => int_batch("page_size", 4096),
        "page_count" => int_batch("page_count", db_page_count(db, query)?),
        "freelist_count" => int_batch("freelist_count", 0),
        "cache_size" => int_batch("cache_size", -2000),
        "automatic_index" => int_batch("automatic_index", 1),
        "defer_foreign_keys" => int_batch("defer_foreign_keys", 0),
        "recursive_triggers" => {
            if let Some(value) = parse_optional_i64(arg.as_deref())? {
                db.set_recursive_triggers(value != 0);
            }
            int_batch(
                "recursive_triggers",
                i64::from(db.trigger_config().recursive_triggers),
            )
        }
        "trusted_schema" => int_batch("trusted_schema", 0),
        "wal_checkpoint" => pragma_wal_checkpoint(session, db, query),
        "optimize" => {
            analyze_all(session, db, query)?;
            session.clear_cache();
            strings_batch("optimize", vec!["ok".to_string()])
        }
        _other => empty_batch(),
    }
}

fn parse_pragma_body(body: &str, lower_body: &str) -> Result<(String, Option<String>)> {
    let (raw_name, raw_arg) = if let Some(open) = lower_body.find('(') {
        let close = lower_body
            .rfind(')')
            .ok_or_else(|| MongrelQueryError::Schema("expected closing ')' in PRAGMA".into()))?;
        (&body[..open], Some(&body[open + 1..close]))
    } else if let Some(eq) = lower_body.find('=') {
        (&body[..eq], Some(&body[eq + 1..]))
    } else {
        (body, None)
    };
    let mut name = raw_name.trim().to_ascii_lowercase();
    if let Some((schema, local)) = name.split_once('.') {
        if schema != "main" {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported PRAGMA schema qualifier {schema:?}"
            )));
        }
        name = local.to_string();
    }
    let arg = raw_arg
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(strip_identifier)
        .transpose()?
        .map(str::to_string);
    Ok((name, arg))
}

fn required_pragma_arg<'a>(name: &str, arg: Option<&'a str>) -> Result<&'a str> {
    arg.ok_or_else(|| MongrelQueryError::Schema(format!("expected PRAGMA {name}(<name>)")))
}

fn schema_version(db: &Arc<Database>) -> i64 {
    db.table_names()
        .into_iter()
        .filter_map(|table| {
            table_schema(db, &table)
                .ok()
                .map(|schema| schema.schema_id as i64)
        })
        .max()
        .unwrap_or(0)
}

fn parse_optional_i64(value: Option<&str>) -> Result<Option<i64>> {
    value
        .map(|value| {
            value
                .trim()
                .parse::<i64>()
                .map_err(|e| MongrelQueryError::Schema(format!("invalid PRAGMA integer: {e}")))
        })
        .transpose()
}

fn db_pragma_file(db: &Arc<Database>) -> std::path::PathBuf {
    db.root().join("_meta").join("sql_pragmas.json")
}

fn get_db_pragma_i64(db: &Arc<Database>, key: &str) -> Result<i64> {
    if let Some(value) = db.sql_pragma_i64(key)? {
        return Ok(value);
    }
    let path = db_pragma_file(db);
    let Ok(bytes) = fs::read(&path) else {
        return Ok(0);
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    Ok(value.get(key).and_then(|v| v.as_i64()).unwrap_or(0))
}

fn db_page_count(db: &Arc<Database>, query: &RegisteredSqlQuery) -> Result<i64> {
    let mut scanned = 0_usize;
    let bytes = dir_size(db.root(), query, &mut scanned)?;
    Ok(bytes.div_ceil(4096) as i64)
}

fn dir_size(path: &Path, query: &RegisteredSqlQuery, scanned: &mut usize) -> Result<u64> {
    let mut total = 0_u64;
    let Ok(entries) = fs::read_dir(path) else {
        return Ok(0);
    };
    for entry in entries {
        if (*scanned).is_multiple_of(COMMAND_CHECKPOINT_ROWS) {
            query.checkpoint()?;
        }
        *scanned += 1;
        let entry = entry.map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        let metadata = entry
            .metadata()
            .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        if metadata.is_dir() {
            total = total.saturating_add(dir_size(&entry.path(), query, scanned)?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn describe_table(db: &Arc<Database>, table: &str) -> Result<RecordBatch> {
    let schema = table_schema(db, table)?;
    let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let types: Vec<String> = schema
        .columns
        .iter()
        .map(|c| format!("{:?}", c.ty))
        .collect();
    let nullable: Vec<bool> = schema
        .columns
        .iter()
        .map(|c| c.flags.contains(ColumnFlags::NULLABLE))
        .collect();
    let primary: Vec<bool> = schema
        .columns
        .iter()
        .map(|c| c.flags.contains(ColumnFlags::PRIMARY_KEY))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("column_name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("nullable", ArrowDataType::Boolean, false),
            Field::new("primary_key", ArrowDataType::Boolean, false),
        ])),
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(types)),
            Arc::new(BooleanArray::from(nullable)),
            Arc::new(BooleanArray::from(primary)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_table_info(db: &Arc<Database>, table: &str) -> Result<RecordBatch> {
    let schema = table_schema(db, table)?;
    let cid: Vec<i64> = (0..schema.columns.len()).map(|i| i as i64).collect();
    let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let types: Vec<String> = schema
        .columns
        .iter()
        .map(|c| format!("{:?}", c.ty))
        .collect();
    let not_null: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(!c.flags.contains(ColumnFlags::NULLABLE)))
        .collect();
    let dflt_value: Vec<Option<String>> = schema.columns.iter().map(|_| None).collect();
    let pk: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(c.flags.contains(ColumnFlags::PRIMARY_KEY)))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("notnull", ArrowDataType::Int64, false),
            Field::new("dflt_value", ArrowDataType::Utf8, true),
            Field::new("pk", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(not_null)),
            Arc::new(StringArray::from(dflt_value)),
            Arc::new(Int64Array::from(pk)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_table_xinfo(db: &Arc<Database>, table: &str) -> Result<RecordBatch> {
    let schema = table_schema(db, table)?;
    let hidden_names = db
        .external_table(table)
        .map(|entry| entry.hidden_columns)
        .unwrap_or_default();
    let cid: Vec<i64> = (0..schema.columns.len()).map(|i| i as i64).collect();
    let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let types: Vec<String> = schema
        .columns
        .iter()
        .map(|c| format!("{:?}", c.ty))
        .collect();
    let not_null: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(!c.flags.contains(ColumnFlags::NULLABLE)))
        .collect();
    let dflt_value: Vec<Option<String>> = schema.columns.iter().map(|_| None).collect();
    let pk: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(c.flags.contains(ColumnFlags::PRIMARY_KEY)))
        .collect();
    let hidden: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(hidden_names.iter().any(|name| name == &c.name)))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("notnull", ArrowDataType::Int64, false),
            Field::new("dflt_value", ArrowDataType::Utf8, true),
            Field::new("pk", ArrowDataType::Int64, false),
            Field::new("hidden", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(not_null)),
            Arc::new(StringArray::from(dflt_value)),
            Arc::new(Int64Array::from(pk)),
            Arc::new(Int64Array::from(hidden)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_table_list(
    session: &MongrelSession,
    db: &Arc<Database>,
    filter: Option<&str>,
) -> Result<RecordBatch> {
    let mut schema_name = Vec::new();
    let mut names = Vec::new();
    let mut ty = Vec::new();
    let mut ncol = Vec::new();
    let mut wr = Vec::new();
    let mut strict = Vec::new();
    let filter = filter.map(str::to_ascii_lowercase);

    for table in db.table_names() {
        if filter
            .as_deref()
            .is_some_and(|needle| table.to_ascii_lowercase() != needle)
        {
            continue;
        }
        let table_schema = table_schema(db, &table)?;
        schema_name.push("main".to_string());
        names.push(table);
        ty.push("table".to_string());
        ncol.push(table_schema.columns.len() as i64);
        wr.push(0_i64);
        strict.push(0_i64);
    }

    for table in db.external_tables() {
        if filter
            .as_deref()
            .is_some_and(|needle| table.name.to_ascii_lowercase() != needle)
        {
            continue;
        }
        schema_name.push("main".to_string());
        names.push(table.name);
        ty.push("external".to_string());
        ncol.push(table.declared_schema.columns.len() as i64);
        wr.push(0_i64);
        strict.push(0_i64);
    }

    for view in session.views.lock().keys() {
        if filter
            .as_deref()
            .is_some_and(|needle| view.to_ascii_lowercase() != needle)
        {
            continue;
        }
        schema_name.push("main".to_string());
        names.push(view.clone());
        ty.push("view".to_string());
        ncol.push(0_i64);
        wr.push(0_i64);
        strict.push(0_i64);
    }

    let mut order = (0..names.len()).collect::<Vec<_>>();
    order.sort_by(|a, b| names[*a].cmp(&names[*b]));
    let schema_name: Vec<String> = order.iter().map(|i| schema_name[*i].clone()).collect();
    let names: Vec<String> = order.iter().map(|i| names[*i].clone()).collect();
    let ty: Vec<String> = order.iter().map(|i| ty[*i].clone()).collect();
    let ncol: Vec<i64> = order.iter().map(|i| ncol[*i]).collect();
    let wr: Vec<i64> = order.iter().map(|i| wr[*i]).collect();
    let strict: Vec<i64> = order.iter().map(|i| strict[*i]).collect();

    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("schema", ArrowDataType::Utf8, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("ncol", ArrowDataType::Int64, false),
            Field::new("wr", ArrowDataType::Int64, false),
            Field::new("strict", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(schema_name)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(ty)),
            Arc::new(Int64Array::from(ncol)),
            Arc::new(Int64Array::from(wr)),
            Arc::new(Int64Array::from(strict)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_index_list(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    query: &RegisteredSqlQuery,
) -> Result<RecordBatch> {
    if let Some(entry) = db.external_table(table) {
        let indexes = session
            .external_modules
            .external_table_indexes(db, &entry, query)?;
        let seq: Vec<i64> = (0..indexes.len()).map(|i| i as i64).collect();
        let names: Vec<String> = indexes.iter().map(|i| i.name.clone()).collect();
        let unique: Vec<i64> = indexes.iter().map(|i| i64::from(i.unique)).collect();
        let origin: Vec<String> = indexes.iter().map(|_| "m".to_string()).collect();
        let partial: Vec<i64> = indexes.iter().map(|i| i64::from(i.partial)).collect();
        return index_list_batch(seq, names, unique, origin, partial);
    }
    let schema = table_schema(db, table)?;
    let seq: Vec<i64> = (0..schema.indexes.len()).map(|i| i as i64).collect();
    let names: Vec<String> = schema.indexes.iter().map(|i| i.name.clone()).collect();
    let unique: Vec<i64> = schema.indexes.iter().map(|_| 0).collect();
    let origin: Vec<String> = schema.indexes.iter().map(|_| "c".to_string()).collect();
    let partial: Vec<i64> = schema
        .indexes
        .iter()
        .map(|idx| idx.predicate.is_some() as i64)
        .collect();
    index_list_batch(seq, names, unique, origin, partial)
}

fn index_list_batch(
    seq: Vec<i64>,
    names: Vec<String>,
    unique: Vec<i64>,
    origin: Vec<String>,
    partial: Vec<i64>,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("unique", ArrowDataType::Int64, false),
            Field::new("origin", ArrowDataType::Utf8, false),
            Field::new("partial", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seq)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(unique)),
            Arc::new(StringArray::from(origin)),
            Arc::new(Int64Array::from(partial)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_index_xinfo(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: &str,
    query: &RegisteredSqlQuery,
) -> Result<RecordBatch> {
    if let Some((entry, def)) = find_external_module_index(session, db, index, query)? {
        return pragma_external_index_xinfo(&entry, &def);
    }
    let table = find_index_table(db, index)?.ok_or_else(|| {
        MongrelQueryError::Schema(format!("index {index:?} does not exist in this database"))
    })?;
    let schema = table_schema(db, &table)?;
    let defs = index_defs(&schema, index);
    let seqno: Vec<i64> = (0..defs.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = defs.iter().map(|idx| idx.column_id as i64).collect();
    let names: Vec<String> = defs
        .iter()
        .map(|idx| column_name(&schema, idx.column_id))
        .collect();
    let desc: Vec<i64> = defs.iter().map(|_| 0_i64).collect();
    let coll: Vec<String> = defs.iter().map(|_| "BINARY".to_string()).collect();
    let key: Vec<i64> = defs.iter().map(|_| 1_i64).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("desc", ArrowDataType::Int64, false),
            Field::new("coll", ArrowDataType::Utf8, false),
            Field::new("key", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(desc)),
            Arc::new(StringArray::from(coll)),
            Arc::new(Int64Array::from(key)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_index_info(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: &str,
    query: &RegisteredSqlQuery,
) -> Result<RecordBatch> {
    if let Some((entry, def)) = find_external_module_index(session, db, index, query)? {
        return pragma_external_index_info(&entry, &def);
    }
    let table = find_index_table(db, index)?.ok_or_else(|| {
        MongrelQueryError::Schema(format!("index {index:?} does not exist in this database"))
    })?;
    let schema = table_schema(db, &table)?;
    let defs = index_defs(&schema, index);
    let seqno: Vec<i64> = (0..defs.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = defs.iter().map(|idx| idx.column_id as i64).collect();
    let names: Vec<String> = defs
        .iter()
        .map(|idx| {
            schema
                .columns
                .iter()
                .find(|col| col.id == idx.column_id)
                .map(|col| col.name.clone())
                .unwrap_or_default()
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn find_external_module_index(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: &str,
    query: &RegisteredSqlQuery,
) -> Result<Option<(ExternalTableEntry, ExternalModuleIndex)>> {
    for entry in db.external_tables() {
        if let Some(def) = session
            .external_modules
            .external_table_indexes(db, &entry, query)?
            .into_iter()
            .find(|def| def.name == index)
        {
            return Ok(Some((entry, def)));
        }
    }
    Ok(None)
}

fn pragma_external_index_xinfo(
    entry: &ExternalTableEntry,
    def: &ExternalModuleIndex,
) -> Result<RecordBatch> {
    let seqno: Vec<i64> = (0..def.column_ids.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = def.column_ids.iter().map(|id| *id as i64).collect();
    let names: Vec<String> = def
        .column_ids
        .iter()
        .map(|id| column_name(&entry.declared_schema, *id))
        .collect();
    let desc: Vec<i64> = def.column_ids.iter().map(|_| 0_i64).collect();
    let coll: Vec<String> = def
        .column_ids
        .iter()
        .map(|_| "BINARY".to_string())
        .collect();
    let key: Vec<i64> = def.column_ids.iter().map(|_| 1_i64).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("desc", ArrowDataType::Int64, false),
            Field::new("coll", ArrowDataType::Utf8, false),
            Field::new("key", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(desc)),
            Arc::new(StringArray::from(coll)),
            Arc::new(Int64Array::from(key)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_external_index_info(
    entry: &ExternalTableEntry,
    def: &ExternalModuleIndex,
) -> Result<RecordBatch> {
    let seqno: Vec<i64> = (0..def.column_ids.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = def.column_ids.iter().map(|id| *id as i64).collect();
    let names: Vec<String> = def
        .column_ids
        .iter()
        .map(|id| column_name(&entry.declared_schema, *id))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn index_defs<'a>(schema: &'a CoreSchema, index: &str) -> Vec<&'a IndexDef> {
    let prefix = format!("{index}_");
    schema
        .indexes
        .iter()
        .filter(|idx| idx.name == index || idx.name.starts_with(&prefix))
        .collect()
}

fn pragma_foreign_key_list(db: &Arc<Database>, table: &str) -> Result<RecordBatch> {
    let schema = table_schema(db, table)?;
    let mut id = Vec::new();
    let mut seq = Vec::new();
    let mut ref_table = Vec::new();
    let mut from = Vec::new();
    let mut to = Vec::new();
    let mut on_update = Vec::new();
    let mut on_delete = Vec::new();
    let mut match_kind = Vec::new();
    for fk in &schema.constraints.foreign_keys {
        let parent = table_schema(db, &fk.ref_table).ok();
        for (i, (from_id, to_id)) in fk.columns.iter().zip(fk.ref_columns.iter()).enumerate() {
            id.push(fk.id as i64);
            seq.push(i as i64);
            ref_table.push(fk.ref_table.clone());
            from.push(column_name(&schema, *from_id));
            to.push(
                parent
                    .as_ref()
                    .map(|s| column_name(s, *to_id))
                    .unwrap_or_else(|| to_id.to_string()),
            );
            on_update.push(format_fk_action(fk.on_update).to_string());
            on_delete.push(format_fk_action(fk.on_delete).to_string());
            match_kind.push("NONE".to_string());
        }
    }
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("table", ArrowDataType::Utf8, false),
            Field::new("from", ArrowDataType::Utf8, false),
            Field::new("to", ArrowDataType::Utf8, false),
            Field::new("on_update", ArrowDataType::Utf8, false),
            Field::new("on_delete", ArrowDataType::Utf8, false),
            Field::new("match", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(id)) as ArrayRef,
            Arc::new(Int64Array::from(seq)),
            Arc::new(StringArray::from(ref_table)),
            Arc::new(StringArray::from(from)),
            Arc::new(StringArray::from(to)),
            Arc::new(StringArray::from(on_update)),
            Arc::new(StringArray::from(on_delete)),
            Arc::new(StringArray::from(match_kind)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_foreign_key_check(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: Option<&str>,
    query: &RegisteredSqlQuery,
) -> Result<RecordBatch> {
    let mut child_table = Vec::new();
    let mut rowid = Vec::new();
    let mut parent_table = Vec::new();
    let mut fkid = Vec::new();
    let tables = match table {
        Some(table) => vec![table.to_string()],
        None => db.table_names(),
    };

    let mut row_visits = 0_usize;
    let mut total_key_bytes = 0_usize;
    let mut output_bytes = 0_usize;
    for table_name in tables {
        query.checkpoint()?;
        let schema = table_schema(db, &table_name)?;
        for fk in &schema.constraints.foreign_keys {
            query.checkpoint()?;
            let mut parent_keys = HashSet::new();
            let mut parent_key_bytes = 0_usize;
            let parent_available = match db.table(&fk.ref_table) {
                Ok(_) => {
                    for_each_visible_row_controlled(
                        session,
                        db,
                        &fk.ref_table,
                        query,
                        |_schema, row| {
                            charge_foreign_key_check_work(
                                &mut row_visits,
                                1,
                                FOREIGN_KEY_CHECK_MAX_ROW_VISITS,
                                "foreign key check row visits",
                            )?;
                            let Some(key) = foreign_key_composite_key(&row, &fk.ref_columns)?
                            else {
                                return Ok(());
                            };
                            charge_foreign_key_check_work(
                                &mut total_key_bytes,
                                key.len(),
                                FOREIGN_KEY_CHECK_TOTAL_KEY_BYTES_LIMIT,
                                "foreign key check total key bytes",
                            )?;
                            if !parent_keys.contains(&key) {
                                let allocation_bytes = key
                                    .len()
                                    .saturating_add(std::mem::size_of::<Vec<u8>>())
                                    .saturating_add(2 * std::mem::size_of::<usize>());
                                charge_foreign_key_check_work(
                                    &mut parent_key_bytes,
                                    allocation_bytes,
                                    FOREIGN_KEY_CHECK_PARENT_KEY_BYTES_LIMIT,
                                    "foreign key check parent key bytes",
                                )?;
                                parent_keys.insert(key);
                            }
                            Ok(())
                        },
                    )?;
                    true
                }
                Err(mongreldb_core::MongrelError::NotFound(_)) => false,
                Err(error) => return Err(error.into()),
            };

            for_each_visible_row_controlled(session, db, &table_name, query, |_schema, row| {
                charge_foreign_key_check_work(
                    &mut row_visits,
                    1,
                    FOREIGN_KEY_CHECK_MAX_ROW_VISITS,
                    "foreign key check row visits",
                )?;
                let Some(key) = foreign_key_composite_key(&row, &fk.columns)? else {
                    return Ok(());
                };
                charge_foreign_key_check_work(
                    &mut total_key_bytes,
                    key.len(),
                    FOREIGN_KEY_CHECK_TOTAL_KEY_BYTES_LIMIT,
                    "foreign key check total key bytes",
                )?;
                if parent_available && parent_keys.contains(&key) {
                    return Ok(());
                }
                let requested_violations = child_table.len().saturating_add(1);
                if requested_violations > FOREIGN_KEY_CHECK_MAX_VIOLATIONS {
                    return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                        resource: "foreign key check violations",
                        requested: requested_violations,
                        limit: FOREIGN_KEY_CHECK_MAX_VIOLATIONS,
                    }
                    .into());
                }
                let output_row_bytes = (2 * std::mem::size_of::<String>())
                    .saturating_add(2 * std::mem::size_of::<i64>())
                    .saturating_add(table_name.len())
                    .saturating_add(fk.ref_table.len())
                    .saturating_mul(2);
                charge_foreign_key_check_work(
                    &mut output_bytes,
                    output_row_bytes,
                    FOREIGN_KEY_CHECK_OUTPUT_BYTES_LIMIT,
                    "foreign key check output bytes",
                )?;
                child_table.push(table_name.clone());
                rowid.push(row.row_id.0 as i64);
                parent_table.push(fk.ref_table.clone());
                fkid.push(fk.id as i64);
                Ok(())
            })?;
        }
    }

    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("table", ArrowDataType::Utf8, false),
            Field::new("rowid", ArrowDataType::Int64, false),
            Field::new("parent", ArrowDataType::Utf8, false),
            Field::new("fkid", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(child_table)) as ArrayRef,
            Arc::new(Int64Array::from(rowid)),
            Arc::new(StringArray::from(parent_table)),
            Arc::new(Int64Array::from(fkid)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn charge_foreign_key_check_work(
    used: &mut usize,
    amount: usize,
    limit: usize,
    resource: &'static str,
) -> Result<()> {
    let requested = used.saturating_add(amount);
    if requested > limit {
        return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
            resource,
            requested,
            limit,
        }
        .into());
    }
    *used = requested;
    Ok(())
}

fn foreign_key_composite_key(row: &Row, columns: &[u16]) -> Result<Option<Vec<u8>>> {
    let mut encoded_bytes = 0_usize;
    for column in columns {
        let Some(value) = row.columns.get(column) else {
            return Ok(None);
        };
        if matches!(value, Value::Null) {
            return Ok(None);
        }
        encoded_bytes = encoded_bytes
            .saturating_add(std::mem::size_of::<u32>())
            .saturating_add(value_encoded_key_len(value));
        if encoded_bytes > FOREIGN_KEY_CHECK_KEY_BYTES_LIMIT {
            return Err(mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "foreign key check composite key bytes",
                requested: encoded_bytes,
                limit: FOREIGN_KEY_CHECK_KEY_BYTES_LIMIT,
            }
            .into());
        }
    }
    let mut key = Vec::with_capacity(encoded_bytes);
    for column in columns {
        let value = match row.columns.get(column) {
            Some(value) => value,
            None => return Ok(None),
        };
        let encoded = value.encode_key();
        let encoded_len = u32::try_from(encoded.len()).map_err(|_| {
            mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "foreign key check composite key bytes",
                requested: encoded.len(),
                limit: FOREIGN_KEY_CHECK_KEY_BYTES_LIMIT,
            }
        })?;
        key.extend_from_slice(&encoded_len.to_be_bytes());
        key.extend_from_slice(&encoded);
    }
    Ok(Some(key))
}

fn value_encoded_key_len(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int64(_) | Value::Float64(_) => 8,
        Value::Bytes(value) | Value::Json(value) => value.len(),
        Value::Embedding(value) => value.len().saturating_mul(std::mem::size_of::<f32>()),
        Value::Decimal(_) | Value::Uuid(_) => 16,
        Value::Interval { .. } => 20,
    }
}

fn row_deep_bytes(row: &Row) -> usize {
    let bucket_bytes =
        std::mem::size_of::<(u16, Value)>().saturating_add(2 * std::mem::size_of::<usize>());
    row.columns
        .capacity()
        .saturating_mul(bucket_bytes)
        .saturating_add(std::mem::size_of::<Row>())
        .saturating_add(
            row.columns
                .values()
                .map(value_encoded_key_len)
                .fold(0_usize, usize::saturating_add),
        )
}

fn cells_deep_bytes(cells: &[(u16, Value)]) -> usize {
    cells
        .len()
        .saturating_mul(std::mem::size_of::<(u16, Value)>())
        .saturating_add(
            cells
                .iter()
                .map(|(_, value)| value_encoded_key_len(value))
                .fold(0_usize, usize::saturating_add),
        )
}

fn column_name(schema: &CoreSchema, id: u16) -> String {
    schema
        .columns
        .iter()
        .find(|col| col.id == id)
        .map(|col| col.name.clone())
        .unwrap_or_else(|| id.to_string())
}

fn format_fk_action(action: mongreldb_core::constraint::FkAction) -> &'static str {
    match action {
        mongreldb_core::constraint::FkAction::Restrict => "RESTRICT",
        mongreldb_core::constraint::FkAction::Cascade => "CASCADE",
        mongreldb_core::constraint::FkAction::SetNull => "SET NULL",
    }
}

fn pragma_database_list(db: &Arc<Database>) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("file", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            Arc::new(StringArray::from(vec!["main"])),
            Arc::new(StringArray::from(vec![db.root().display().to_string()])),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_function_list() -> Result<RecordBatch> {
    let mut rows = crate::extended_sql_functions::extended_sql_function_names()
        .into_iter()
        .filter(|name| {
            *name != "json_each"
                && *name != "json_tree"
                && *name != "jsonb_each"
                && *name != "jsonb_tree"
                && *name != "series"
        })
        .map(|name| (name.to_string(), "s".to_string(), -1_i64))
        .collect::<Vec<_>>();
    rows.push(("ann_search".to_string(), "s".to_string(), 3));
    rows.push(("sparse_match".to_string(), "s".to_string(), 3));
    rows.push(("rtree_intersects".to_string(), "s".to_string(), 8));
    rows.push(("json_each".to_string(), "t".to_string(), 2));
    rows.push(("json_tree".to_string(), "t".to_string(), 2));
    rows.push(("jsonb_each".to_string(), "t".to_string(), 2));
    rows.push(("jsonb_tree".to_string(), "t".to_string(), 2));
    rows.push(("series".to_string(), "t".to_string(), 3));
    for (name, narg) in [
        ("avg", 1),
        ("count", -1),
        ("group_concat", -1),
        ("max", -1),
        ("median", 1),
        ("min", -1),
        ("percentile", 2),
        ("percentile_cont", 2),
        ("percentile_disc", 2),
        ("string_agg", 2),
        ("sum", 1),
        ("total", 1),
    ] {
        rows.push((name.to_string(), "w".to_string(), narg));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let names = rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>();
    let builtin = rows.iter().map(|_| 1_i64).collect::<Vec<_>>();
    let kind = rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>();
    let enc = rows.iter().map(|_| "utf8".to_string()).collect::<Vec<_>>();
    let narg = rows.iter().map(|r| r.2).collect::<Vec<_>>();
    let flags = rows.iter().map(|_| 0_i64).collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("builtin", ArrowDataType::Int64, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("enc", ArrowDataType::Utf8, false),
            Field::new("narg", ArrowDataType::Int64, false),
            Field::new("flags", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(builtin)),
            Arc::new(StringArray::from(kind)),
            Arc::new(StringArray::from(enc)),
            Arc::new(Int64Array::from(narg)),
            Arc::new(Int64Array::from(flags)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_module_list(session: &MongrelSession) -> Result<RecordBatch> {
    strings_batch("name", session.external_modules.names())
}

fn pragma_trigger_list(db: &Arc<Database>) -> Result<RecordBatch> {
    let triggers = db.triggers();
    let seq = (0..triggers.len() as i64).collect::<Vec<_>>();
    let names = triggers
        .iter()
        .map(|trigger| trigger.name.clone())
        .collect::<Vec<_>>();
    let target_type = triggers
        .iter()
        .map(|trigger| match &trigger.target {
            TriggerTarget::Table(_) => "table".to_string(),
            TriggerTarget::View(_) => "view".to_string(),
        })
        .collect::<Vec<_>>();
    let target = triggers
        .iter()
        .map(|trigger| match &trigger.target {
            TriggerTarget::Table(name) | TriggerTarget::View(name) => name.clone(),
        })
        .collect::<Vec<_>>();
    let timing = triggers
        .iter()
        .map(|trigger| trigger_timing_name(trigger.timing).to_string())
        .collect::<Vec<_>>();
    let event = triggers
        .iter()
        .map(|trigger| trigger_event_name(trigger.event).to_string())
        .collect::<Vec<_>>();
    let enabled = triggers
        .iter()
        .map(|trigger| i64::from(trigger.enabled))
        .collect::<Vec<_>>();
    let created_epoch = triggers
        .iter()
        .map(|trigger| trigger.created_epoch as i64)
        .collect::<Vec<_>>();
    let updated_epoch = triggers
        .iter()
        .map(|trigger| trigger.updated_epoch as i64)
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("target_type", ArrowDataType::Utf8, false),
            Field::new("target", ArrowDataType::Utf8, false),
            Field::new("timing", ArrowDataType::Utf8, false),
            Field::new("event", ArrowDataType::Utf8, false),
            Field::new("enabled", ArrowDataType::Int64, false),
            Field::new("created_epoch", ArrowDataType::Int64, false),
            Field::new("updated_epoch", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seq)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(target_type)),
            Arc::new(StringArray::from(target)),
            Arc::new(StringArray::from(timing)),
            Arc::new(StringArray::from(event)),
            Arc::new(Int64Array::from(enabled)),
            Arc::new(Int64Array::from(created_epoch)),
            Arc::new(Int64Array::from(updated_epoch)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn trigger_timing_name(timing: TriggerTiming) -> &'static str {
    match timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    }
}

fn trigger_event_name(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    }
}

fn pragma_collation_list() -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["BINARY"])),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_compile_options() -> Result<RecordBatch> {
    strings_batch(
        "compile_options",
        vec![
            "DATAFUSION_54".to_string(),
            "EXTENDED_SQL_FUNCTIONS".to_string(),
            "JSON_TABLE_FUNCTIONS".to_string(),
            "LOG_STRUCTURED_STORAGE".to_string(),
            "MVCC".to_string(),
            "WAL".to_string(),
        ],
    )
}

fn pragma_wal_checkpoint(
    session: &MongrelSession,
    db: &Arc<Database>,
    query: &RegisteredSqlQuery,
) -> Result<RecordBatch> {
    for (table_index, table) in db.table_names().into_iter().enumerate() {
        command_checkpoint(session, query, table_index)?;
        let handle = db.table(&table)?;
        let mut table = handle.lock();
        run_controlled_durable_with_optional_epoch(session, query, |before_commit| {
            let (epoch, changed) =
                table.flush_with_outcome_controlled(query.control(), before_commit)?;
            Ok(((), changed.then_some(epoch.0)))
        })?;
    }
    run_gc(session, db, query)?;
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("busy", ArrowDataType::Int64, false),
            Field::new("log", ArrowDataType::Int64, false),
            Field::new("checkpointed", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0_i64])),
            Arc::new(Int64Array::from(vec![0_i64])),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn check_batch(db: &Arc<Database>, query: &RegisteredSqlQuery) -> Result<RecordBatch> {
    let issues = controlled_check(db, query)?;
    let severity: Vec<String> = issues.iter().map(|i| i.severity.clone()).collect();
    let table: Vec<String> = issues.iter().map(|i| i.table_name.clone()).collect();
    let description: Vec<String> = issues.iter().map(|i| i.description.clone()).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("severity", ArrowDataType::Utf8, false),
            Field::new("table_name", ArrowDataType::Utf8, false),
            Field::new("description", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from(severity)) as ArrayRef,
            Arc::new(StringArray::from(table)),
            Arc::new(StringArray::from(description)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_check_batch(
    db: &Arc<Database>,
    query: &RegisteredSqlQuery,
    column_name: &str,
) -> Result<RecordBatch> {
    let issues = controlled_check(db, query)?;
    let values = if issues.is_empty() {
        vec!["ok".to_string()]
    } else {
        issues
            .iter()
            .map(|issue| {
                format!(
                    "{}: {}: {}",
                    issue.severity, issue.table_name, issue.description
                )
            })
            .collect()
    };
    strings_batch(column_name, values)
}

fn controlled_check(
    db: &Arc<Database>,
    query: &RegisteredSqlQuery,
) -> Result<Vec<mongreldb_core::CheckIssue>> {
    match db.check_controlled(query.control()) {
        Ok(issues) => Ok(issues),
        Err(error) => {
            query.checkpoint()?;
            Err(error.into())
        }
    }
}

fn int_batch(name: &str, value: i64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            name,
            ArrowDataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn empty_batch() -> Result<RecordBatch> {
    Ok(RecordBatch::new_empty(Arc::new(ArrowSchema::empty())))
}

fn strings_batch(name: &str, values: Vec<String>) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            name,
            ArrowDataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn json_batch(name: &str, values: Vec<String>) -> Result<RecordBatch> {
    strings_batch(name, values)
}

// ── Recursive CTE evaluation ───────────────────────────────────────────────

/// Evaluate a `WITH RECURSIVE` CTE using the standard naive algorithm:
///
/// 1. Execute the base (non-recursive) query.
/// 2. Register the result as a temp table named after the CTE.
/// 3. Repeatedly execute the recursive arm (which references the CTE name),
///    appending new rows to the temp table, until no new rows are produced.
/// 4. Execute the outer query against the final temp table.
///
/// DataFusion v54's built-in recursive CTE support has a column-alias bug, so
/// we intercept and evaluate ourselves before DataFusion sees the query.
async fn try_recursive_cte(
    session: &MongrelSession,
    sql: &str,
    query: &RegisteredSqlQuery,
) -> Result<Option<Vec<RecordBatch>>> {
    use sqlparser::ast::{SetExpr, Statement};
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    let statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|e| MongrelQueryError::Schema(format!("recursive CTE parse error: {e}")))?;
    if statements.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "recursive CTE requires exactly one statement".into(),
        ));
    }

    let Some(statement) = statements.into_iter().next() else {
        return Err(MongrelQueryError::InvalidQueryState(
            "recursive CTE parser returned no statement".into(),
        ));
    };
    let Statement::Query(parsed_query) = statement else {
        return Ok(None);
    };

    let with = parsed_query
        .with
        .as_ref()
        .ok_or_else(|| MongrelQueryError::Schema("expected WITH RECURSIVE".into()))?;
    if !with.recursive {
        return Ok(None);
    }
    if with.cte_tables.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "only a single recursive CTE is supported".into(),
        ));
    }

    let cte = &with.cte_tables[0];
    let cte_name = cte.alias.name.to_string();
    let cte_body = &*cte.query.body;

    // Extract the CTE's declared column names (e.g. `counter(n)` → ["n"]).
    // These are used to rename the temp table's columns so the recursive arm
    // can reference them by name (DataFusion doesn't propagate CTE column aliases).
    let cte_col_names: Vec<String> = cte
        .alias
        .columns
        .iter()
        .map(|c| c.name.value.clone())
        .collect();

    // Extract base + recursive queries from the UNION [ALL] set operation.
    let (base_sql, recursive_sql, _union_all) = match cte_body {
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
            ..
        } => {
            if !matches!(op, sqlparser::ast::SetOperator::Union) {
                return Err(MongrelQueryError::Schema(
                    "recursive CTE supports UNION only".into(),
                ));
            }
            let is_all = matches!(set_quantifier, sqlparser::ast::SetQuantifier::All);
            let base_sql = format!("SELECT * FROM ({}) AS base", left);
            let recursive_sql = format!("SELECT * FROM ({}) AS recur", right);
            (base_sql, recursive_sql, is_all)
        }
        _ => {
            return Err(MongrelQueryError::Schema(
                "recursive CTE body must be a UNION set operation".into(),
            ));
        }
    };

    // 1. Execute the base query.
    query.checkpoint()?;
    let base_batches = Box::pin(session.run(&base_sql)).await?;
    if base_batches.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let schema = base_batches[0].schema();

    // Rename the batch columns to match the CTE's declared column names so the
    // recursive arm can reference them by name (e.g. `n` instead of `"Int64(1)"`).
    let renamed_schema =
        if !cte_col_names.is_empty() && cte_col_names.len() == schema.fields().len() {
            let new_fields: Vec<_> = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(i, f)| {
                    // Use the CTE's declared column name. If the base query
                    // produced a Null type (e.g. `SELECT NULL`), use Int64
                    // nullable instead — Null can't hold values from the
                    // recursive arm.
                    let dt = match f.data_type() {
                        arrow::datatypes::DataType::Null => arrow::datatypes::DataType::Int64,
                        other => other.clone(),
                    };
                    arrow::datatypes::Field::new(&cte_col_names[i], dt, true)
                })
                .collect();
            std::sync::Arc::new(arrow::datatypes::Schema::new(new_fields))
        } else {
            schema.clone()
        };

    // Re-cast each base batch with the renamed schema. If the base query
    // produced Null-type columns (e.g. `SELECT NULL`), convert them to
    // the target type as all-null arrays.
    let all_batches: Vec<RecordBatch> = base_batches
        .iter()
        .map(|b| {
            let cols: Vec<ArrayRef> = b
                .columns()
                .iter()
                .enumerate()
                .map(|(i, col)| {
                    let target_dt = renamed_schema.field(i).data_type();
                    if col.data_type() == &arrow::datatypes::DataType::Null
                        && target_dt != &arrow::datatypes::DataType::Null
                    {
                        // Replace NullArray with an all-null array of the target type.
                        arrow::array::new_null_array(target_dt, col.len())
                    } else {
                        col.clone()
                    }
                })
                .collect();
            RecordBatch::try_new(renamed_schema.clone(), cols)
                .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
        })
        .collect::<Result<_>>()?;

    let mut all_batches = all_batches;

    // Register initial result under the CTE name.
    let merged = arrow::compute::concat_batches(&renamed_schema, &all_batches)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
    let _ = session.ctx.register_batch(&cte_name, merged.clone());

    // 2. Iteratively execute the recursive arm using the semi-naive
    //    (delta-only) algorithm: each iteration evaluates the recursive arm
    //    against only the NEW rows from the previous iteration (the "delta"),
    //    not the full accumulated table. This avoids infinite recursion with
    //    UNION ALL and is the standard evaluation strategy.
    let max_iterations = 10_000;
    let mut delta_merged = merged.clone();
    for iteration in 0..max_iterations {
        command_checkpoint(session, query, iteration)?;
        // Register ONLY the delta (new rows) as the CTE name, so the recursive
        // arm sees only the rows added in the previous iteration.
        let _ = session.ctx.deregister_table(&cte_name);
        let _ = session.ctx.register_batch(&cte_name, delta_merged.clone());
        session.clear_cache();
        session.plan_cache.lock().clear();

        let new_batches = Box::pin(session.run(&recursive_sql)).await?;
        query.checkpoint()?;
        let new_rows: usize = new_batches.iter().map(|b| b.num_rows()).sum();
        if new_rows == 0 {
            break;
        }

        // Re-cast new batches to the CTE's renamed schema.
        let recast_batches: Vec<RecordBatch> = new_batches
            .iter()
            .map(|b| {
                RecordBatch::try_new(renamed_schema.clone(), b.columns().to_vec())
                    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
            })
            .collect::<Result<_>>()?;

        // The new delta is the recast batches from this iteration.
        delta_merged = arrow::compute::concat_batches(&renamed_schema, &recast_batches)
            .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;

        // For UNION (non-ALL), deduplicate the delta against the accumulated
        // set before accumulating. Register the full set, run SELECT DISTINCT,
        // and use only the genuinely new rows as the delta.
        if !_union_all && !all_batches.is_empty() {
            let full_merged = arrow::compute::concat_batches(&renamed_schema, &all_batches)
                .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
            let _ = session.ctx.deregister_table(&cte_name);
            let _ = session.ctx.register_batch(&cte_name, full_merged);
            session.clear_cache();
            session.plan_cache.lock().clear();

            // Deduplicate everything (accumulated + new delta).
            let combined: Vec<RecordBatch> = all_batches
                .iter()
                .cloned()
                .chain(std::iter::once(delta_merged.clone()))
                .collect();
            let combined_merged = arrow::compute::concat_batches(&renamed_schema, &combined)
                .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
            let _ = session.ctx.deregister_table(&cte_name);
            let _ = session.ctx.register_batch(&cte_name, combined_merged);
            session.clear_cache();
            session.plan_cache.lock().clear();

            let deduped =
                Box::pin(session.run(&format!("SELECT DISTINCT * FROM {}", cte_name))).await?;
            let _deduped_merged = arrow::compute::concat_batches(&renamed_schema, &deduped)
                .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
            let prev_total: usize = all_batches.iter().map(|b| b.num_rows()).sum();
            all_batches = deduped;
            let new_total: usize = all_batches.iter().map(|b| b.num_rows()).sum();
            if new_total == prev_total {
                break; // No genuinely new rows.
            }
            // The delta is just the new rows = deduped total - previous total.
            // Since we can't easily extract just the new rows from the deduped
            // set, use the full deduped set as the next delta. This is less
            // efficient but correct — the recursive arm will re-evaluate against
            // the full set and SELECT DISTINCT will prevent accumulation.
            let deduped_merged = arrow::compute::concat_batches(&renamed_schema, &all_batches)
                .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
            delta_merged = deduped_merged;
        } else {
            // UNION ALL: just accumulate.
            all_batches.extend(recast_batches);
        }
    }

    // Register the full accumulated result as the CTE name for the outer query.
    let full_merged = arrow::compute::concat_batches(&renamed_schema, &all_batches)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
    query.checkpoint()?;
    let _ = session.ctx.deregister_table(&cte_name);
    let _ = session.ctx.register_batch(&cte_name, full_merged);

    // Clear caches so the outer query sees the freshly registered CTE table.
    session.clear_cache();
    session.plan_cache.lock().clear();

    // 3. Execute the outer query (after the CTE definition).
    let outer_select = extract_outer_query(sql)?;
    let result = Box::pin(session.run(&outer_select)).await?;
    query.checkpoint()?;
    Ok(Some(result))
}

/// Extract the outer query from a WITH RECURSIVE statement.
/// Given `WITH RECURSIVE name(cols) AS (...) <outer>`, return `<outer>`.
fn extract_outer_query(sql: &str) -> Result<String> {
    // Find the matching closing parenthesis for the CTE's AS (...).
    // Strategy: scan for "AS (" (case-insensitive), then track depth.
    let lower = sql.to_ascii_lowercase();
    let as_pos = lower.find(" as (").ok_or_else(|| {
        MongrelQueryError::Schema("could not find 'AS (' in recursive CTE".into())
    })?;
    let paren_start = as_pos + 4; // position of the '('
    let mut depth = 0i32;
    let mut paren_end = paren_start;
    for (i, c) in sql[paren_start..].char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    paren_end = paren_start + i + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    let outer = sql[paren_end..].trim().trim_end_matches(';').trim();
    if outer.is_empty() {
        return Err(MongrelQueryError::Schema(
            "recursive CTE requires an outer query".into(),
        ));
    }
    Ok(outer.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctas_arrow_values_are_typed_and_timestamp_safe() {
        use arrow::array::{Array, Date32Array, LargeStringArray, TimestampSecondArray};

        let large: Arc<dyn Array> = Arc::new(LargeStringArray::from(vec![Some("large text")]));
        assert_eq!(
            arrow_cell_to_value(&large, 0).unwrap(),
            Value::Bytes(b"large text".to_vec())
        );

        let date: Arc<dyn Array> = Arc::new(Date32Array::from(vec![Some(42)]));
        assert_eq!(arrow_cell_to_value(&date, 0).unwrap(), Value::Int64(42));

        let timestamp: Arc<dyn Array> = Arc::new(TimestampSecondArray::from(vec![Some(2)]));
        assert_eq!(
            arrow_cell_to_value(&timestamp, 0).unwrap(),
            Value::Int64(2_000_000_000)
        );

        let overflow: Arc<dyn Array> = Arc::new(TimestampSecondArray::from(vec![Some(i64::MAX)]));
        assert!(matches!(
            arrow_cell_to_value(&overflow, 0),
            Err(MongrelQueryError::Arrow(message))
                if message == "timestamp overflows nanosecond storage"
        ));
    }

    #[test]
    fn spilled_operation_rewalks_honor_cancellation() {
        let registry = Arc::new(crate::SqlQueryRegistry::new(
            1,
            1,
            1024,
            std::time::Duration::from_secs(60),
        ));
        let query = registry
            .register(crate::SqlQueryOptions::default())
            .unwrap();
        let mut ops = PendingSqlOps::default();
        for row_id in 0..1_100 {
            ops.push(PendingSqlOp::Delete {
                table: "items".into(),
                row_id: mongreldb_core::RowId(row_id),
            })
            .unwrap();
        }
        query.request_cancel(mongreldb_core::CancellationReason::ClientRequest);

        assert!(matches!(
            logical_changes_spooled(&mut ops, &query),
            Err(MongrelQueryError::QueryCancelled { .. })
        ));

        let mut staged = PendingSqlOps::default();
        assert!(matches!(
            staged.append_from(&mut ops, &query),
            Err(MongrelQueryError::QueryCancelled { .. })
        ));
        assert!(staged.is_empty());
    }

    #[tokio::test]
    async fn command_batch_wait_is_cancel_wakeable() {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        let registry = Arc::new(crate::SqlQueryRegistry::new(
            1,
            1,
            1024,
            std::time::Duration::from_secs(60),
        ));
        let query = registry
            .register(crate::SqlQueryOptions::default())
            .unwrap();
        let cancel_query = query.clone();
        let schema = Arc::new(ArrowSchema::empty());
        let pending = futures::stream::pending::<datafusion::error::Result<RecordBatch>>();
        let mut stream: MongrelRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, pending));
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            cancel_query.request_cancel(mongreldb_core::CancellationReason::ClientRequest);
        });

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            next_command_batch(&mut stream, &query),
        )
        .await
        .expect("blocked stream must wake on cancellation")
        .unwrap_err();
        assert!(matches!(error, MongrelQueryError::QueryCancelled { .. }));
    }

    fn spill_bytes(ops: &mut PendingSqlOps) -> Vec<u8> {
        let spill = ops.spill.as_mut().expect("operations must spill");
        spill.file.flush().unwrap();
        let mut file = spill.file.try_clone().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut raw = Vec::new();
        file.read_to_end(&mut raw).unwrap();
        raw
    }

    fn spill_nonces(raw: &[u8]) -> Vec<[u8; 12]> {
        let mut nonces = Vec::new();
        let mut offset = 0;
        while offset < raw.len() {
            let frame_len =
                u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            nonces.push(raw[offset..offset + 12].try_into().unwrap());
            offset += 12 + frame_len;
        }
        nonces
    }

    fn spill_frame_ranges(raw: &[u8]) -> Vec<std::ops::Range<usize>> {
        let mut frames = Vec::new();
        let mut offset = 0;
        while offset < raw.len() {
            let start = offset;
            let frame_len =
                u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + 12 + frame_len;
            frames.push(start..offset);
        }
        frames
    }

    fn replace_spill_bytes(ops: &mut PendingSqlOps, raw: &[u8]) {
        let spill = ops.spill.as_mut().unwrap();
        spill.file.set_len(0).unwrap();
        spill.file.seek(SeekFrom::Start(0)).unwrap();
        spill.file.write_all(raw).unwrap();
        spill.file.flush().unwrap();
    }

    #[test]
    fn staged_sql_operations_spill_and_rollback_without_a_count_cap() {
        let mut ops = PendingSqlOps::default();
        for index in 0..10_001 {
            ops.push(PendingSqlOp::Truncate {
                table: format!("t{index}"),
                changes: index as u64,
            })
            .unwrap();
        }
        assert!(ops.spill.is_some());
        assert_eq!(ops.reader().unwrap().count(), 10_001);

        let checkpoint = ops.checkpoint().unwrap();
        ops.push(PendingSqlOp::Truncate {
            table: "rolled_back".into(),
            changes: 0,
        })
        .unwrap();
        ops.truncate(checkpoint).unwrap();
        assert_eq!(ops.reader().unwrap().count(), 10_001);
    }

    #[test]
    fn staged_sql_spill_is_encrypted_and_bounded_by_memory_bytes() {
        let secret = "super_secret_staged_table";
        let mut ops = PendingSqlOps::default();
        for index in 0..9 {
            ops.push(PendingSqlOp::ExternalState {
                table: if index == 0 {
                    secret.into()
                } else {
                    format!("table_{index}")
                },
                state: vec![index as u8; 1024 * 1024],
                changes: 1,
            })
            .unwrap();
        }
        let spill = ops.spill.as_mut().expect("byte bound must spill");
        spill.file.flush().unwrap();
        let mut file = spill.file.try_clone().unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut raw = Vec::new();
        file.read_to_end(&mut raw).unwrap();
        assert!(!raw
            .windows(secret.len())
            .any(|window| window == secret.as_bytes()));
        assert_eq!(ops.reader().unwrap().count(), 9);
    }

    #[test]
    fn staged_sql_spill_never_reuses_nonce_after_rollback() {
        let mut ops = PendingSqlOps::default();
        for index in 0..9 {
            ops.push(PendingSqlOp::ExternalState {
                table: format!("table_{index}"),
                state: vec![index as u8; 1024 * 1024],
                changes: 1,
            })
            .unwrap();
        }
        let checkpoint = ops.checkpoint().unwrap();
        ops.push(PendingSqlOp::Truncate {
            table: "first_tail".into(),
            changes: 1,
        })
        .unwrap();
        let first_tail_nonce = *spill_nonces(&spill_bytes(&mut ops)).last().unwrap();

        ops.truncate(checkpoint).unwrap();
        ops.push(PendingSqlOp::Truncate {
            table: "replacement_tail".into(),
            changes: 1,
        })
        .unwrap();
        let replacement_nonce = *spill_nonces(&spill_bytes(&mut ops)).last().unwrap();

        assert_ne!(first_tail_nonce, replacement_nonce);
        assert_eq!(ops.reader().unwrap().count(), 10);
    }

    #[test]
    fn staged_sql_spill_rejects_ciphertext_tampering() {
        let mut ops = PendingSqlOps::default();
        for index in 0..9 {
            ops.push(PendingSqlOp::ExternalState {
                table: format!("table_{index}"),
                state: vec![index as u8; 1024 * 1024],
                changes: 1,
            })
            .unwrap();
        }
        let spill = ops.spill.as_mut().unwrap();
        spill.file.seek(SeekFrom::Start(16)).unwrap();
        let mut byte = [0_u8; 1];
        spill.file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x80;
        spill.file.seek(SeekFrom::Start(16)).unwrap();
        spill.file.write_all(&byte).unwrap();
        spill.file.flush().unwrap();

        assert!(matches!(
            ops.reader().unwrap().next().unwrap(),
            Err(MongrelQueryError::InvalidQueryState(message))
                if message.contains("authentication failed")
        ));
    }

    #[test]
    fn staged_sql_spill_rejects_frame_reordering_and_replay() {
        let mut ops = PendingSqlOps::default();
        for index in 0..9 {
            ops.push(PendingSqlOp::ExternalState {
                table: format!("table_{index}"),
                state: vec![index as u8; 1024 * 1024],
                changes: 1,
            })
            .unwrap();
        }
        let original = spill_bytes(&mut ops);
        let frames = spill_frame_ranges(&original);

        let mut reordered = Vec::with_capacity(original.len());
        reordered.extend_from_slice(&original[frames[1].clone()]);
        reordered.extend_from_slice(&original[frames[0].clone()]);
        reordered.extend_from_slice(&original[frames[2].start..]);
        replace_spill_bytes(&mut ops, &reordered);
        assert!(matches!(
            ops.reader().unwrap().next().unwrap(),
            Err(MongrelQueryError::InvalidQueryState(_))
        ));

        let mut replayed = Vec::with_capacity(original.len());
        replayed.extend_from_slice(&original[frames[0].clone()]);
        replayed.extend_from_slice(&original[frames[0].clone()]);
        replayed.extend_from_slice(&original[frames[2].start..]);
        replace_spill_bytes(&mut ops, &replayed);
        let mut reader = ops.reader().unwrap();
        assert!(reader.next().unwrap().is_ok());
        assert!(matches!(
            reader.next().unwrap(),
            Err(MongrelQueryError::InvalidQueryState(message))
                if message.contains("nonce order")
        ));
    }

    #[tokio::test]
    async fn failed_statement_cleanup_keeps_transaction_poisoned_until_rollback() {
        let directory = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::create(directory.path()).unwrap());
        let session = crate::MongrelSession::open(database).unwrap();
        session
            .run("CREATE TABLE items (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        session.run("BEGIN").await.unwrap();
        session.run("INSERT INTO items VALUES (1)").await.unwrap();
        session.run("SAVEPOINT before_failure").await.unwrap();
        let query = session
            .register_query(crate::SqlQueryOptions::default())
            .unwrap();
        let guard = crate::TransactionStatementGuard::new(&session, &query, true).unwrap();
        {
            let mut transaction = session.transaction.lock();
            let ops = transaction.staged_ops.as_mut().unwrap();
            for index in 0..1_025 {
                ops.push(PendingSqlOp::Truncate {
                    table: format!("staged_{index}"),
                    changes: 0,
                })
                .unwrap();
            }
            let spill = ops.spill.as_mut().unwrap();
            spill.file.seek(SeekFrom::Start(16)).unwrap();
            let mut byte = [0_u8; 1];
            spill.file.read_exact(&mut byte).unwrap();
            byte[0] ^= 0x80;
            spill.file.seek(SeekFrom::Start(16)).unwrap();
            spill.file.write_all(&byte).unwrap();
            spill.file.flush().unwrap();
        }
        drop(guard);

        {
            let transaction = session.transaction.lock();
            assert!(transaction.staged_ops.is_some());
            assert!(transaction.aborted);
            assert!(transaction.savepoints.is_empty());
        }
        assert!(matches!(
            session.run("INSERT INTO items VALUES (2)").await,
            Err(MongrelQueryError::TransactionAborted)
        ));
        assert!(matches!(
            session.run("COMMIT").await,
            Err(MongrelQueryError::TransactionAborted)
        ));
        session.run("ROLLBACK").await.unwrap();
        let transaction = session.transaction.lock();
        assert!(transaction.staged_ops.is_none());
        assert!(!transaction.aborted);
    }

    #[test]
    fn staged_sql_transaction_has_a_total_spill_bound() {
        let mut ops = PendingSqlOps {
            total_bytes: PENDING_SQL_OPS_TOTAL_BYTES_LIMIT,
            ..PendingSqlOps::default()
        };
        assert!(matches!(
            ops.push(PendingSqlOp::Truncate {
                table: "over_limit".into(),
                changes: 1,
            }),
            Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "staged SQL transaction bytes",
                    limit: PENDING_SQL_OPS_TOTAL_BYTES_LIMIT,
                    ..
                }
            ))
        ));
    }

    #[test]
    fn create_index_with_options_parses_typed_values() {
        let statement = Parser::parse_sql(
            &GenericDialect {},
            "CREATE INDEX mh ON docs USING minhash (members) WITH (permutations = 64, bands = 16)",
        )
        .unwrap()
        .remove(0);
        let Statement::CreateIndex(index) = statement else {
            panic!("expected CREATE INDEX")
        };
        assert_eq!(
            index_kind_from_sql(index.using.as_ref()).unwrap(),
            IndexKind::MinHash
        );
        let options = parse_index_options(IndexKind::MinHash, &index.with).unwrap();
        let minhash = options.minhash.unwrap();
        assert_eq!(minhash.permutations, 64);
        assert_eq!(minhash.bands, 16);

        let statement = Parser::parse_sql(
            &GenericDialect {},
            "CREATE INDEX mh ON docs USING lsh (members)",
        )
        .unwrap()
        .remove(0);
        let Statement::CreateIndex(index) = statement else {
            panic!("expected CREATE INDEX")
        };
        assert_eq!(
            index_kind_from_sql(index.using.as_ref()).unwrap(),
            IndexKind::MinHash
        );

        let statement = Parser::parse_sql(
            &GenericDialect {},
            "CREATE INDEX ann ON docs USING ann (embedding) WITH (m = 8, ef_construction = 32, ef_search = 17, quantization = 'binary_sign')",
        )
        .unwrap()
        .remove(0);
        let Statement::CreateIndex(index) = statement else {
            panic!("expected CREATE INDEX")
        };
        let ann = parse_index_options(IndexKind::Ann, &index.with)
            .unwrap()
            .ann
            .unwrap();
        assert_eq!((ann.m, ann.ef_construction, ann.ef_search), (8, 32, 17));
        assert_eq!(ann.quantization, AnnQuantization::BinarySign);
    }
}

#[cfg(test)]
mod ssi_sql_tests {
    //! Stage 1B follow-up: SQL-layer SSI integration. SQL transactions stage
    //! their writes and commit them in one core transaction; at `Serializable`
    //! the tables a transaction scanned are replayed into the commit
    //! transaction's `track_predicate_read`, so core certification aborts a
    //! commit whose read set a concurrent commit invalidated (write skew).

    use super::*;
    use crate::MongrelSession;

    fn test_database() -> (tempfile::TempDir, Arc<Database>) {
        let directory = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::create(directory.path()).unwrap());
        (directory, database)
    }

    async fn create_tables(database: &Arc<Database>) {
        let bootstrap = MongrelSession::open(Arc::clone(database)).unwrap();
        bootstrap
            .run("CREATE TABLE t1 (id BIGINT PRIMARY KEY, v BIGINT)")
            .await
            .unwrap();
        bootstrap
            .run("CREATE TABLE t2 (id BIGINT PRIMARY KEY, v BIGINT)")
            .await
            .unwrap();
    }

    fn open_session(database: &Arc<Database>) -> MongrelSession {
        MongrelSession::open(Arc::clone(database)).unwrap()
    }

    async fn begin_serializable(session: &MongrelSession) {
        session.run("BEGIN").await.unwrap();
        session
            .run("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
    }

    /// Classic write skew through SQL: each transaction reads the table the
    /// other writes. The later commit must abort with
    /// `MongrelError::SerializationFailure` — raised natively by core's
    /// certification and carried through the query error boundary with the
    /// precise taxonomy category 8.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn serializable_write_skew_aborts_with_serialization_failure() {
        let (_directory, database) = test_database();
        create_tables(&database).await;
        let session_one = open_session(&database);
        let session_two = Arc::new(open_session(&database));

        begin_serializable(&session_one).await;
        begin_serializable(&session_two).await;
        session_one.run("SELECT * FROM t2").await.unwrap();
        session_one
            .run("INSERT INTO t1 VALUES (1, 10)")
            .await
            .unwrap();
        session_two.run("SELECT * FROM t1").await.unwrap();
        session_two
            .run("INSERT INTO t2 VALUES (2, 20)")
            .await
            .unwrap();

        // Pause session_two's commit after its core transaction is built (the
        // read epoch is pinned) but before certification, so session_one's
        // commit lands deterministically inside the certification window.
        let (paused_tx, paused_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = parking_lot::Mutex::new(release_rx);
        session_two.set_test_hook(Some(Arc::new(move |point| {
            if point == SqlTestHookPoint::BeforeTransactionCommit {
                paused_tx.send(()).expect("test listens");
                release_rx.lock().recv().expect("test releases");
            }
        })));
        let commit = tokio::spawn({
            let session_two = Arc::clone(&session_two);
            async move { session_two.run("COMMIT").await }
        });
        paused_rx.recv().unwrap();

        session_one.run("COMMIT").await.unwrap();
        release_tx.send(()).unwrap();

        let outcome = commit.await.unwrap();
        session_two.set_test_hook(None);
        match outcome {
            Err(MongrelQueryError::Core(mongreldb_core::MongrelError::SerializationFailure {
                message,
            })) => {
                assert!(
                    message.contains("invalidated this transaction's reads"),
                    "core certification detail survives: {message}"
                );
                // The precise taxonomy category reaches the SQL caller: 8
                // (SerializationFailure), not the generic conflict category.
                assert_eq!(
                    mongreldb_core::MongrelError::SerializationFailure { message }
                        .category()
                        .code(),
                    8
                );
            }
            other => panic!("expected SerializationFailure, got {other:?}"),
        }

        // The aborted transaction must roll back before the session is usable;
        // a retried transaction with no concurrent writer commits.
        session_two.run("ROLLBACK").await.unwrap();
        begin_serializable(&session_two).await;
        session_two.run("SELECT * FROM t1").await.unwrap();
        session_two
            .run("INSERT INTO t2 VALUES (2, 20)")
            .await
            .unwrap();
        session_two.run("COMMIT").await.unwrap();
        let batches = session_two.run("SELECT * FROM t2").await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1);
    }

    /// The same interleave at the default RepeatableRead commits both sides:
    /// the write sets are disjoint, so neither first-committer-wins nor any
    /// read certification fires.
    #[tokio::test]
    async fn repeatable_read_write_skew_commits_both_sides() {
        let (_directory, database) = test_database();
        create_tables(&database).await;
        let session_one = open_session(&database);
        let session_two = open_session(&database);

        for session in [&session_one, &session_two] {
            session.run("BEGIN").await.unwrap();
        }
        session_one.run("SELECT * FROM t2").await.unwrap();
        session_one
            .run("INSERT INTO t1 VALUES (1, 10)")
            .await
            .unwrap();
        session_two.run("SELECT * FROM t1").await.unwrap();
        session_two
            .run("INSERT INTO t2 VALUES (2, 20)")
            .await
            .unwrap();

        session_one.run("COMMIT").await.unwrap();
        session_two.run("COMMIT").await.unwrap();

        let batches = session_one.run("SELECT * FROM t1").await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1);
        let batches = session_two.run("SELECT * FROM t2").await.unwrap();
        let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 1);
    }

    /// `SET TRANSACTION` / `SET SESSION CHARACTERISTICS` / `START
    /// TRANSACTION` drive the isolation level the commit path reads back.
    #[tokio::test]
    async fn sql_isolation_surface_drives_the_commit_isolation() {
        let (_directory, database) = test_database();
        create_tables(&database).await;
        let session = open_session(&database);

        // Default is RepeatableRead, core's default.
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::RepeatableRead
        );

        // SET TRANSACTION pends for the next BEGIN.
        session
            .run("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::Serializable
        );
        session.run("BEGIN").await.unwrap();
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::Serializable
        );
        session.run("INSERT INTO t1 VALUES (1, 10)").await.unwrap();
        session.run("COMMIT").await.unwrap();
        // COMMIT consumed the pending override; the session default returns.
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::RepeatableRead
        );

        // START TRANSACTION carries the mode inline; ROLLBACK clears it.
        session
            .run("START TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::Serializable
        );
        session.run("ROLLBACK").await.unwrap();
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::RepeatableRead
        );

        // SET SESSION CHARACTERISTICS sets the default for later
        // transactions; an explicit mode overrides it for one transaction.
        session
            .run("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
            .unwrap();
        session.run("BEGIN").await.unwrap();
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::ReadCommitted
        );
        session.run("ROLLBACK").await.unwrap();
        session
            .run("START TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .unwrap();
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::RepeatableRead
        );
        session.run("ROLLBACK").await.unwrap();

        // Unsupported modes fail closed and leave the session state untouched.
        assert!(session
            .run("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
            .await
            .is_err());
        assert!(session.run("START TRANSACTION READ ONLY").await.is_err());
        assert_eq!(
            session.commit_isolation_and_predicate_reads().0,
            mongreldb_core::IsolationLevel::ReadCommitted
        );
    }

    /// Serializable transactions record table scans (DataFusion path) and
    /// UPDATE/DELETE matched-row scans; weaker levels record nothing, and
    /// `EXPLAIN` (which never executes its scan) records nothing.
    #[tokio::test]
    async fn serializable_transaction_records_scans_and_dml_read_sides() {
        let (_directory, database) = test_database();
        create_tables(&database).await;
        let session = open_session(&database);
        session.run("INSERT INTO t1 VALUES (1, 10)").await.unwrap();

        // RepeatableRead records nothing.
        session.run("BEGIN").await.unwrap();
        session.run("SELECT * FROM t1").await.unwrap();
        session
            .run("UPDATE t1 SET v = 99 WHERE id >= 1")
            .await
            .unwrap();
        assert!(session.commit_isolation_and_predicate_reads().1.is_empty());
        session.run("ROLLBACK").await.unwrap();

        // EXPLAIN never executes its scan: nothing to certify.
        begin_serializable(&session).await;
        session.run("EXPLAIN SELECT * FROM t1").await.unwrap();
        assert!(session.commit_isolation_and_predicate_reads().1.is_empty());
        session.run("ROLLBACK").await.unwrap();

        // Serializable records the SELECT scan and the UPDATE matched-row scan.
        begin_serializable(&session).await;
        session.run("SELECT * FROM t1").await.unwrap();
        session
            .run("UPDATE t1 SET v = 99 WHERE id >= 1")
            .await
            .unwrap();
        let (level, reads) = session.commit_isolation_and_predicate_reads();
        assert_eq!(level, mongreldb_core::IsolationLevel::Serializable);
        assert_eq!(reads, vec!["t1".to_string()]);
        session.run("COMMIT").await.unwrap();
        // COMMIT consumed the recorded set.
        assert!(session.commit_isolation_and_predicate_reads().1.is_empty());
    }

    /// Two writers interleaved on a LockManager deadlock; the deterministic
    /// victim's `LockError::Deadlock` surfaces at the SQL error boundary as
    /// `MongrelError::Deadlock` (taxonomy category 9). Direct manager probe
    /// (the SQL FOR UPDATE path also uses these exclusive row keys).
    #[test]
    fn lock_manager_deadlock_surfaces_as_deadlock_at_the_sql_boundary() {
        use mongreldb_core::locks::{LockKey, LockManager, LockMode, LockRequest};
        use mongreldb_core::{ExecutionControl, RowId};

        fn request(txn_id: u64) -> LockRequest {
            LockRequest::new(txn_id, LockMode::Exclusive, ExecutionControl::new(None))
        }

        let manager = Arc::new(LockManager::new());
        let key_a = LockKey::row(1, RowId(1));
        let key_b = LockKey::row(1, RowId(2));
        manager.acquire(key_a.clone(), request(1)).unwrap();
        manager.acquire(key_b.clone(), request(2)).unwrap();

        // t1 blocks on B; t2's request for A closes the cycle. Victim
        // selection (youngest) dooms t2 regardless of which request lands
        // first, so no sequencing is needed.
        let waiter = std::thread::spawn({
            let manager = Arc::clone(&manager);
            let key_b = key_b.clone();
            move || manager.acquire(key_b, request(1))
        });
        let error = manager.acquire(key_a, request(2)).unwrap_err();
        assert!(
            matches!(
                error,
                mongreldb_core::locks::LockError::Deadlock { victim: 2, .. }
            ),
            "youngest transaction is the deterministic victim: {error:?}"
        );

        let query_error = MongrelQueryError::from(mongreldb_core::MongrelError::from(error));
        assert!(
            matches!(
                query_error,
                MongrelQueryError::Core(mongreldb_core::MongrelError::Deadlock { victim: 2, .. })
            ),
            "the SQL boundary surfaces the precise deadlock variant: {query_error:?}"
        );

        // The victim aborts; the survivor's wait completes.
        manager.release_all(2);
        assert_eq!(waiter.join().unwrap(), Ok(()));
        manager.release_all(1);
    }

    #[tokio::test]
    async fn select_for_update_requires_open_transaction_and_locks_rows() {
        use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
        use mongreldb_core::Database;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_table(
            "items",
            Schema {
                schema_id: 1,
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
                        name: "v".into(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                indexes: vec![],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            },
        )
        .unwrap();
        {
            let handle = db.table("items").unwrap();
            let mut t = handle.lock();
            t.put(vec![
                (1, mongreldb_core::Value::Int64(1)),
                (2, mongreldb_core::Value::Int64(10)),
            ])
            .unwrap();
            t.commit().unwrap();
        }

        let session = MongrelSession::open(Arc::clone(&db)).unwrap();
        // Outside a transaction FOR UPDATE fails closed.
        let err = session
            .run("SELECT * FROM items FOR UPDATE")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("open transaction") || err.to_string().contains("BEGIN"),
            "unexpected: {err}"
        );

        session.run("BEGIN").await.unwrap();
        session
            .run("SELECT * FROM items FOR UPDATE")
            .await
            .expect("FOR UPDATE inside BEGIN acquires row locks");
        session.run("COMMIT").await.unwrap();
    }
}
