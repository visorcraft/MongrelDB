use crate::{MongrelQueryError, Result, SqlTestHook};
use mongreldb_core::{CancellationReason, ExecutionControl};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

const DEFAULT_MAX_ACTIVE: usize = 1_024;
const DEFAULT_MAX_FINISHED: usize = 2_048;
const DEFAULT_MAX_FINISHED_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_FINISHED_TTL: Duration = Duration::from_secs(60);
const MAX_METADATA_BYTES: usize = 256;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId([u8; 16]);

impl QueryId {
    pub fn random() -> Result<Self> {
        let mut bytes = [0; 16];
        getrandom::getrandom(&mut bytes).map_err(|error| {
            MongrelQueryError::Core(mongreldb_core::MongrelError::Other(format!(
                "query id randomness failed: {error}"
            )))
        })?;
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Display for QueryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for QueryId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl FromStr for QueryId {
    type Err = MongrelQueryError;

    fn from_str(value: &str) -> Result<Self> {
        if value.len() != 32 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::InvalidArgument(
                    "query id must be exactly 32 hexadecimal characters".into(),
                ),
            ));
        }
        let mut bytes = [0; 16];
        for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(chunk).map_err(|_| {
                MongrelQueryError::Core(mongreldb_core::MongrelError::InvalidArgument(
                    "query id is not valid UTF-8".into(),
                ))
            })?;
            bytes[index] = u8::from_str_radix(text, 16).map_err(|_| {
                MongrelQueryError::Core(mongreldb_core::MongrelError::InvalidArgument(
                    "query id contains invalid hexadecimal".into(),
                ))
            })?;
        }
        Ok(Self(bytes))
    }
}

impl serde::Serialize for QueryId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for QueryId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <String as serde::Deserialize>::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SqlQueryOptions {
    pub query_id: Option<QueryId>,
    pub timeout: Option<Duration>,
    pub owner: Option<String>,
    pub session_id: Option<String>,
    pub parent_control: Option<ExecutionControl>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SqlQueryPhase {
    Queued = 0,
    Planning = 1,
    Executing = 2,
    Streaming = 3,
    Serializing = 4,
    CommitCritical = 5,
    Cancelling = 6,
    Completed = 7,
    Failed = 8,
    Cancelled = 9,
}

impl SqlQueryPhase {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Queued,
            1 => Self::Planning,
            2 => Self::Executing,
            3 => Self::Streaming,
            4 => Self::Serializing,
            5 => Self::CommitCritical,
            6 => Self::Cancelling,
            7 => Self::Completed,
            8 => Self::Failed,
            9 => Self::Cancelled,
            _ => Self::Failed,
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    Accepted,
    AlreadyCancelling,
    TooLate,
    AlreadyFinished,
    NotFound,
}

#[derive(Debug, Clone)]
pub struct QueryStatus {
    pub query_id: QueryId,
    pub owner: Option<String>,
    pub session_id: Option<String>,
    pub phase: SqlQueryPhase,
    pub started_at: Instant,
    pub deadline: Option<Instant>,
    pub operation: String,
    pub sql_fingerprint: [u8; 32],
    pub cancellation_reason: CancellationReason,
    pub committed: bool,
    pub completed_statements: usize,
    pub statement_index: usize,
    pub cancel_requested_at: Option<Instant>,
    pub queue_duration: Duration,
    pub planning_duration: Duration,
    pub execution_duration: Duration,
    pub serialization_duration: Duration,
    pub cancel_requested_phase: Option<SqlQueryPhase>,
    pub cancel_observed_phase: Option<SqlQueryPhase>,
    pub commit_fence_outcome: CommitFenceOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFenceOutcome {
    NotReached,
    CancelWon,
    CommitWon,
}

#[derive(Debug, Clone)]
struct QueryTrace {
    phase_started_at: Instant,
    queue_duration: Duration,
    planning_duration: Duration,
    execution_duration: Duration,
    serialization_duration: Duration,
    cancel_requested_phase: Option<SqlQueryPhase>,
    cancel_observed_phase: Option<SqlQueryPhase>,
    commit_fence_outcome: CommitFenceOutcome,
}

impl QueryTrace {
    fn new(started_at: Instant) -> Self {
        Self {
            phase_started_at: started_at,
            queue_duration: Duration::ZERO,
            planning_duration: Duration::ZERO,
            execution_duration: Duration::ZERO,
            serialization_duration: Duration::ZERO,
            cancel_requested_phase: None,
            cancel_observed_phase: None,
            commit_fence_outcome: CommitFenceOutcome::NotReached,
        }
    }

    fn transition(&mut self, phase: SqlQueryPhase) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.phase_started_at);
        match phase {
            SqlQueryPhase::Queued => self.queue_duration += elapsed,
            SqlQueryPhase::Planning => self.planning_duration += elapsed,
            SqlQueryPhase::Executing | SqlQueryPhase::Streaming | SqlQueryPhase::CommitCritical => {
                self.execution_duration += elapsed
            }
            SqlQueryPhase::Serializing => self.serialization_duration += elapsed,
            SqlQueryPhase::Cancelling
            | SqlQueryPhase::Completed
            | SqlQueryPhase::Failed
            | SqlQueryPhase::Cancelled => {}
        }
        self.phase_started_at = now;
    }
}

#[derive(Debug)]
struct RegisteredQuery {
    id: QueryId,
    owner: Option<String>,
    session_id: Option<String>,
    control: ExecutionControl,
    phase: AtomicU8,
    started_at: Instant,
    deadline: Option<Instant>,
    operation: Mutex<String>,
    sql_fingerprint: Mutex<[u8; 32]>,
    committed: AtomicBool,
    completed_statements: AtomicUsize,
    statement_index: AtomicUsize,
    cancel_requested_at: Mutex<Option<Instant>>,
    trace: Mutex<QueryTrace>,
}

impl RegisteredQuery {
    fn phase(&self) -> SqlQueryPhase {
        SqlQueryPhase::from_u8(self.phase.load(Ordering::Acquire))
    }

    fn status(&self) -> QueryStatus {
        let mut trace = self.trace.lock().unwrap().clone();
        trace.transition(self.phase());
        QueryStatus {
            query_id: self.id,
            owner: self.owner.clone(),
            session_id: self.session_id.clone(),
            phase: self.phase(),
            started_at: self.started_at,
            deadline: self.deadline,
            operation: self.operation.lock().unwrap().clone(),
            sql_fingerprint: *self.sql_fingerprint.lock().unwrap(),
            cancellation_reason: self.control.reason(),
            committed: self.committed.load(Ordering::Acquire),
            completed_statements: self.completed_statements.load(Ordering::Acquire),
            statement_index: self.statement_index.load(Ordering::Acquire),
            cancel_requested_at: *self.cancel_requested_at.lock().unwrap(),
            queue_duration: trace.queue_duration,
            planning_duration: trace.planning_duration,
            execution_duration: trace.execution_duration,
            serialization_duration: trace.serialization_duration,
            cancel_requested_phase: trace.cancel_requested_phase,
            cancel_observed_phase: trace.cancel_observed_phase,
            commit_fence_outcome: trace.commit_fence_outcome,
        }
    }

    fn record_transition(&self, phase: SqlQueryPhase) {
        self.trace.lock().unwrap().transition(phase);
    }
}

#[derive(Debug, Clone)]
struct FinishedQuery {
    status: QueryStatus,
    finished_at: Instant,
    approximate_bytes: usize,
}

#[derive(Debug, Default)]
struct RegistryState {
    active: HashMap<QueryId, Arc<RegisteredQuery>>,
    finished: VecDeque<FinishedQuery>,
    finished_bytes: usize,
}

#[derive(Debug)]
pub struct SqlQueryRegistry {
    state: Mutex<RegistryState>,
    max_active: usize,
    max_finished: usize,
    max_finished_bytes: usize,
    finished_ttl: Duration,
}

impl Default for SqlQueryRegistry {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_ACTIVE,
            DEFAULT_MAX_FINISHED,
            DEFAULT_MAX_FINISHED_BYTES,
            DEFAULT_FINISHED_TTL,
        )
    }
}

impl SqlQueryRegistry {
    pub fn new(
        max_active: usize,
        max_finished: usize,
        max_finished_bytes: usize,
        finished_ttl: Duration,
    ) -> Self {
        Self {
            state: Mutex::new(RegistryState::default()),
            max_active: max_active.max(1),
            max_finished,
            max_finished_bytes,
            finished_ttl,
        }
    }

    pub fn register(self: &Arc<Self>, options: SqlQueryOptions) -> Result<RegisteredSqlQuery> {
        validate_metadata("owner", options.owner.as_deref())?;
        validate_metadata("session id", options.session_id.as_deref())?;
        let id = match options.query_id {
            Some(id) => id,
            None => QueryId::random()?,
        };
        let deadline = options.timeout.map(|timeout| Instant::now() + timeout);
        let control = match options.parent_control {
            Some(parent) => parent.child_with_deadline(deadline),
            None => ExecutionControl::new(deadline),
        };
        let started_at = Instant::now();
        let query = Arc::new(RegisteredQuery {
            id,
            owner: options.owner,
            session_id: options.session_id,
            deadline: control.deadline(),
            control,
            phase: AtomicU8::new(SqlQueryPhase::Queued as u8),
            started_at,
            operation: Mutex::new("UNKNOWN".into()),
            sql_fingerprint: Mutex::new([0; 32]),
            committed: AtomicBool::new(false),
            completed_statements: AtomicUsize::new(0),
            statement_index: AtomicUsize::new(0),
            cancel_requested_at: Mutex::new(None),
            trace: Mutex::new(QueryTrace::new(started_at)),
        });
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state);
        if state.active.contains_key(&id) {
            return Err(MongrelQueryError::QueryIdConflict { query_id: id });
        }
        if state.active.len() >= self.max_active {
            return Err(MongrelQueryError::QueryRegistryFull);
        }
        state.active.insert(id, Arc::clone(&query));
        Ok(RegisteredSqlQuery {
            registry: Arc::downgrade(self),
            query,
        })
    }

    pub fn cancel(&self, query_id: QueryId) -> CancelOutcome {
        let query = {
            let mut state = self.state.lock().unwrap();
            self.prune_locked(&mut state);
            if let Some(query) = state.active.get(&query_id) {
                Some(Arc::clone(query))
            } else if state
                .finished
                .iter()
                .any(|finished| finished.status.query_id == query_id)
            {
                return CancelOutcome::AlreadyFinished;
            } else {
                return CancelOutcome::NotFound;
            }
        };
        query
            .unwrap()
            .request_cancel(CancellationReason::ClientRequest)
    }

    pub fn status(&self, query_id: QueryId) -> Option<QueryStatus> {
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state);
        state
            .active
            .get(&query_id)
            .map(|query| query.status())
            .or_else(|| {
                state
                    .finished
                    .iter()
                    .find(|finished| finished.status.query_id == query_id)
                    .map(|finished| finished.status.clone())
            })
    }

    pub fn cancel_session(&self, session_id: &str, reason: CancellationReason) -> usize {
        let queries = {
            let state = self.state.lock().unwrap();
            state
                .active
                .values()
                .filter(|query| query.session_id.as_deref() == Some(session_id))
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut accepted = 0;
        for query in queries {
            if query.request_cancel(reason) == CancelOutcome::Accepted {
                accepted += 1;
            }
        }
        accepted
    }

    pub fn cancel_all(&self, reason: CancellationReason) -> usize {
        let queries = self
            .state
            .lock()
            .unwrap()
            .active
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut accepted = 0;
        for query in queries {
            if query.request_cancel(reason) == CancelOutcome::Accepted {
                accepted += 1;
            }
        }
        accepted
    }

    pub fn active_count(&self) -> usize {
        self.state.lock().unwrap().active.len()
    }

    pub fn active_statuses(&self) -> Vec<QueryStatus> {
        self.state
            .lock()
            .unwrap()
            .active
            .values()
            .map(|query| query.status())
            .collect()
    }

    pub fn active_for_session(&self, session_id: &str) -> usize {
        self.state
            .lock()
            .unwrap()
            .active
            .values()
            .filter(|query| query.session_id.as_deref() == Some(session_id))
            .count()
    }

    pub fn queued_count(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .active
            .values()
            .filter(|query| query.phase() == SqlQueryPhase::Queued)
            .count()
    }

    pub fn entry_count(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state);
        state.active.len() + state.finished.len()
    }

    pub fn approximate_bytes(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state);
        state.finished_bytes
            + state
                .active
                .len()
                .saturating_mul(std::mem::size_of::<RegisteredQuery>())
    }

    pub fn finished_count(&self) -> usize {
        let mut state = self.state.lock().unwrap();
        self.prune_locked(&mut state);
        state.finished.len()
    }

    fn finish(&self, query: &Arc<RegisteredQuery>, phase: SqlQueryPhase) {
        debug_assert!(phase.is_terminal());
        let previous = query.phase();
        query.phase.store(phase as u8, Ordering::Release);
        query.record_transition(previous);
        let status = query.status();
        let approximate_bytes = std::mem::size_of::<FinishedQuery>()
            + status.owner.as_ref().map_or(0, String::len)
            + status.session_id.as_ref().map_or(0, String::len)
            + status.operation.len();
        let mut state = self.state.lock().unwrap();
        if state.active.remove(&query.id).is_none() {
            return;
        }
        if self.max_finished > 0 && self.max_finished_bytes > 0 {
            state.finished_bytes = state.finished_bytes.saturating_add(approximate_bytes);
            state.finished.push_back(FinishedQuery {
                status,
                finished_at: Instant::now(),
                approximate_bytes,
            });
        }
        self.prune_locked(&mut state);
    }

    fn prune_locked(&self, state: &mut RegistryState) {
        let now = Instant::now();
        while state.finished.front().is_some_and(|entry| {
            now.saturating_duration_since(entry.finished_at) >= self.finished_ttl
                || state.finished.len() > self.max_finished
                || state.finished_bytes > self.max_finished_bytes
        }) {
            if let Some(entry) = state.finished.pop_front() {
                state.finished_bytes = state.finished_bytes.saturating_sub(entry.approximate_bytes);
            }
        }
    }
}

impl RegisteredQuery {
    fn request_cancel(&self, reason: CancellationReason) -> CancelOutcome {
        loop {
            let phase = self.phase();
            match phase {
                SqlQueryPhase::Queued
                | SqlQueryPhase::Planning
                | SqlQueryPhase::Executing
                | SqlQueryPhase::Streaming
                | SqlQueryPhase::Serializing => {
                    if self
                        .phase
                        .compare_exchange(
                            phase as u8,
                            SqlQueryPhase::Cancelling as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        *self.cancel_requested_at.lock().unwrap() = Some(Instant::now());
                        let mut trace = self.trace.lock().unwrap();
                        trace.transition(phase);
                        trace.cancel_requested_phase = Some(phase);
                        trace.commit_fence_outcome = CommitFenceOutcome::CancelWon;
                        self.control.cancel(reason);
                        return CancelOutcome::Accepted;
                    }
                }
                SqlQueryPhase::Cancelling | SqlQueryPhase::Cancelled => {
                    return CancelOutcome::AlreadyCancelling;
                }
                SqlQueryPhase::CommitCritical => return CancelOutcome::TooLate,
                SqlQueryPhase::Completed | SqlQueryPhase::Failed => {
                    return CancelOutcome::AlreadyFinished;
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct RegisteredSqlQuery {
    registry: Weak<SqlQueryRegistry>,
    query: Arc<RegisteredQuery>,
}

/// Query-specific state attached to a fresh DataFusion `TaskContext` for each
/// execution. Reusable logical and physical plans never own this value.
pub(crate) struct SqlTaskContext {
    query: RegisteredSqlQuery,
    test_hook: Option<SqlTestHook>,
}

impl SqlTaskContext {
    pub(crate) fn new(query: RegisteredSqlQuery, test_hook: Option<SqlTestHook>) -> Self {
        Self { query, test_hook }
    }

    pub(crate) fn query(&self) -> &RegisteredSqlQuery {
        &self.query
    }

    pub(crate) fn test_hook(&self) -> Option<&SqlTestHook> {
        self.test_hook.as_ref()
    }
}

impl RegisteredSqlQuery {
    pub fn id(&self) -> QueryId {
        self.query.id
    }

    pub fn control(&self) -> &ExecutionControl {
        &self.query.control
    }

    pub fn phase(&self) -> SqlQueryPhase {
        self.query.phase()
    }

    pub fn status(&self) -> QueryStatus {
        self.query.status()
    }

    pub fn set_sql_metadata(&self, sql: &str) {
        let normalized = sql.split_whitespace().collect::<Vec<_>>().join(" ");
        let operation = normalized
            .split_whitespace()
            .next()
            .unwrap_or("UNKNOWN")
            .to_ascii_uppercase();
        *self.query.operation.lock().unwrap() = operation;
        *self.query.sql_fingerprint.lock().unwrap() = Sha256::digest(normalized.as_bytes()).into();
    }

    pub fn transition(&self, expected: SqlQueryPhase, next: SqlQueryPhase) -> Result<()> {
        self.query
            .phase
            .compare_exchange(
                expected as u8,
                next as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| self.query.record_transition(expected))
            .map_err(|actual| {
                let actual = SqlQueryPhase::from_u8(actual);
                if actual == SqlQueryPhase::Cancelling {
                    self.cancellation_error()
                } else {
                    MongrelQueryError::InvalidQueryState(format!(
                        "query {} expected {expected:?}, found {actual:?}",
                        self.id()
                    ))
                }
            })
    }

    pub fn enter_commit_critical(&self) -> Result<()> {
        if let Err(error) = self.checkpoint() {
            self.query.trace.lock().unwrap().commit_fence_outcome = CommitFenceOutcome::CancelWon;
            return Err(error);
        }
        self.transition(SqlQueryPhase::Executing, SqlQueryPhase::CommitCritical)?;
        self.query.trace.lock().unwrap().commit_fence_outcome = CommitFenceOutcome::CommitWon;
        Ok(())
    }

    pub fn exit_commit_critical(&self) -> Result<()> {
        self.transition(SqlQueryPhase::CommitCritical, SqlQueryPhase::Executing)
    }

    pub fn begin_serialization(&self) -> Result<()> {
        match self.phase() {
            SqlQueryPhase::Executing => {
                self.transition(SqlQueryPhase::Executing, SqlQueryPhase::Serializing)
            }
            SqlQueryPhase::Streaming => {
                self.transition(SqlQueryPhase::Streaming, SqlQueryPhase::Serializing)
            }
            // A durable COMMIT keeps its fence through response generation.
            SqlQueryPhase::CommitCritical => Ok(()),
            phase => Err(MongrelQueryError::InvalidQueryState(format!(
                "query {} cannot serialize from {phase:?}",
                self.id()
            ))),
        }
    }

    pub fn mark_committed(&self) {
        self.query.committed.store(true, Ordering::Release);
    }

    pub fn begin_statement(&self, index: usize) {
        self.query.statement_index.store(index, Ordering::Release);
    }

    pub fn complete_statement(&self, index: usize) {
        self.query
            .completed_statements
            .store(index.saturating_add(1), Ordering::Release);
    }

    pub fn complete_current_statement(&self) {
        let index = self.query.statement_index.load(Ordering::Acquire);
        self.complete_statement(index);
    }

    pub fn request_cancel(&self, reason: CancellationReason) -> CancelOutcome {
        self.query.request_cancel(reason)
    }

    pub fn checkpoint(&self) -> Result<()> {
        let result = self
            .query
            .control
            .checkpoint()
            .map_err(|error| match error {
                mongreldb_core::MongrelError::DeadlineExceeded => {
                    MongrelQueryError::DeadlineExceeded {
                        query_id: self.id(),
                        timeout_ms: self.query.deadline.map(|deadline| {
                            deadline
                                .saturating_duration_since(self.query.started_at)
                                .as_millis()
                                .min(u128::from(u64::MAX)) as u64
                        }),
                        completed_statements: self
                            .query
                            .completed_statements
                            .load(Ordering::Acquire),
                        cancelled_statement_index: self
                            .query
                            .statement_index
                            .load(Ordering::Acquire),
                    }
                }
                mongreldb_core::MongrelError::Cancelled => self.cancellation_error(),
                other => MongrelQueryError::Core(other),
            });
        if result.is_err() && self.query.control.is_cancelled() {
            let mut trace = self.query.trace.lock().unwrap();
            trace.cancel_observed_phase = trace.cancel_requested_phase.or(Some(self.phase()));
        }
        result
    }

    pub fn complete(&self) {
        self.finish(SqlQueryPhase::Completed);
    }

    pub fn fail(&self) {
        let phase = if self.query.control.is_cancelled() {
            SqlQueryPhase::Cancelled
        } else {
            SqlQueryPhase::Failed
        };
        self.finish(phase);
    }

    fn finish(&self, phase: SqlQueryPhase) {
        if let Some(registry) = self.registry.upgrade() {
            registry.finish(&self.query, phase);
        } else {
            self.query.phase.store(phase as u8, Ordering::Release);
        }
    }

    fn cancellation_error(&self) -> MongrelQueryError {
        match self.query.control.reason() {
            CancellationReason::Deadline => MongrelQueryError::DeadlineExceeded {
                query_id: self.id(),
                timeout_ms: self.query.deadline.map(|deadline| {
                    deadline
                        .saturating_duration_since(self.query.started_at)
                        .as_millis()
                        .min(u128::from(u64::MAX)) as u64
                }),
                completed_statements: self.query.completed_statements.load(Ordering::Acquire),
                cancelled_statement_index: self.query.statement_index.load(Ordering::Acquire),
            },
            reason => MongrelQueryError::QueryCancelled {
                query_id: self.id(),
                reason,
                completed_statements: self.query.completed_statements.load(Ordering::Acquire),
                cancelled_statement_index: self.query.statement_index.load(Ordering::Acquire),
            },
        }
    }
}

pub struct RegisteredQueryGuard {
    query: Option<RegisteredSqlQuery>,
}

impl RegisteredQueryGuard {
    pub fn new(query: RegisteredSqlQuery) -> Self {
        Self { query: Some(query) }
    }

    pub fn query(&self) -> &RegisteredSqlQuery {
        self.query
            .as_ref()
            .expect("registered query guard consumed")
    }

    pub fn complete(mut self) {
        if let Some(query) = self.query.take() {
            query.complete();
        }
    }

    pub fn fail(mut self) {
        if let Some(query) = self.query.take() {
            query.fail();
        }
    }

    pub fn into_query(mut self) -> RegisteredSqlQuery {
        self.query.take().expect("registered query guard consumed")
    }
}

impl Drop for RegisteredQueryGuard {
    fn drop(&mut self) {
        if let Some(query) = self.query.take() {
            query.fail();
        }
    }
}

fn validate_metadata(name: &str, value: Option<&str>) -> Result<()> {
    if value.is_some_and(|value| value.len() > MAX_METADATA_BYTES) {
        return Err(MongrelQueryError::Core(
            mongreldb_core::MongrelError::InvalidArgument(format!(
                "{name} exceeds {MAX_METADATA_BYTES} bytes"
            )),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_ids_are_random_strict_and_round_trip() {
        let first = QueryId::random().unwrap();
        let second = QueryId::random().unwrap();
        assert_ne!(first, second);
        assert_eq!(first.to_string().parse::<QueryId>().unwrap(), first);
        assert!("abc".parse::<QueryId>().is_err());
        assert!("0000000000000000000000000000000z"
            .parse::<QueryId>()
            .is_err());
    }

    #[test]
    fn duplicate_active_ids_are_rejected_and_cleanup_is_bounded() {
        let registry = Arc::new(SqlQueryRegistry::new(1, 1, 1024, Duration::from_secs(60)));
        let id = QueryId::random().unwrap();
        let query = registry
            .register(SqlQueryOptions {
                query_id: Some(id),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        assert!(matches!(
            registry.register(SqlQueryOptions {
                query_id: Some(id),
                ..SqlQueryOptions::default()
            }),
            Err(MongrelQueryError::QueryIdConflict { .. })
        ));
        query.complete();
        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.finished_count(), 1);
    }

    #[test]
    fn cancel_and_commit_fence_have_one_winner() {
        for cancel_first in [true, false] {
            let registry = Arc::new(SqlQueryRegistry::default());
            let query = registry.register(SqlQueryOptions::default()).unwrap();
            query
                .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
                .unwrap();
            if cancel_first {
                assert_eq!(
                    query.request_cancel(CancellationReason::ClientRequest),
                    CancelOutcome::Accepted
                );
                assert!(query.enter_commit_critical().is_err());
                let status = query.status();
                assert_eq!(
                    status.cancel_requested_phase,
                    Some(SqlQueryPhase::Executing)
                );
                assert_eq!(status.cancel_observed_phase, Some(SqlQueryPhase::Executing));
                assert_eq!(status.commit_fence_outcome, CommitFenceOutcome::CancelWon);
            } else {
                query.enter_commit_critical().unwrap();
                assert_eq!(
                    query.request_cancel(CancellationReason::ClientRequest),
                    CancelOutcome::TooLate
                );
                query.mark_committed();
                query.complete();
                let status = registry.status(query.id()).unwrap();
                assert!(status.committed);
                assert_eq!(status.commit_fence_outcome, CommitFenceOutcome::CommitWon);
            }
        }
    }

    #[test]
    fn guard_cleans_up_dropped_execution_without_raw_sql() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry
            .register(SqlQueryOptions {
                owner: Some("alice".into()),
                session_id: Some("session".into()),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        query.set_sql_metadata("SELECT secret FROM docs WHERE token = 'private'");
        let id = query.id();
        drop(RegisteredQueryGuard::new(query));
        let status = registry.status(id).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Failed);
        assert_eq!(status.operation, "SELECT");
        assert_ne!(status.sql_fingerprint, [0; 32]);
    }
}
