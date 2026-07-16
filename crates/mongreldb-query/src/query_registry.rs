use crate::{MongrelQueryError, Result, SqlTestHook};
use mongreldb_core::{CancellationReason, ExecutionControl};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

const DEFAULT_MAX_ACTIVE: usize = 1_024;
const DEFAULT_MAX_FINISHED: usize = 2_048;
const DEFAULT_MAX_FINISHED_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_FINISHED_TTL: Duration = Duration::from_secs(60);
const MAX_METADATA_BYTES: usize = 256;
const APPROXIMATE_RESERVED_ID_BYTES: usize = 128 + 2 * MAX_METADATA_BYTES;

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DurableOutcome {
    pub committed: bool,
    pub committed_statements: usize,
    pub last_commit_epoch: Option<u64>,
    pub first_commit_statement_index: Option<usize>,
    pub last_commit_statement_index: Option<usize>,
}

impl DurableOutcome {
    fn record_commit(&mut self, statement_index: usize, epoch: Option<u64>) {
        self.committed = true;
        if self.last_commit_statement_index != Some(statement_index) {
            self.committed_statements = self.committed_statements.saturating_add(1);
        }
        self.first_commit_statement_index
            .get_or_insert(statement_index);
        self.last_commit_statement_index = Some(statement_index);
        if epoch.is_some() {
            self.last_commit_epoch = epoch;
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SerializationOutcome {
    #[default]
    NotStarted,
    InProgress,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTerminalErrorCategory {
    Cancellation,
    Deadline,
    ResultLimit,
    Serialization,
    Execution,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryTerminalError {
    pub code: String,
    pub category: QueryTerminalErrorCategory,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueryOutcomeState {
    durable: DurableOutcome,
    serialization: SerializationOutcome,
    terminal_error: Option<QueryTerminalError>,
    terminal_state_override: Option<QueryTerminalState>,
    cancellation_reason_override: Option<CancellationReason>,
    phase_override: Option<SqlQueryPhase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryTerminalState {
    OutcomeUnknown,
    Completed,
    FailedBeforeCommit,
    CancelledBeforeCommit,
    DeadlineBeforeCommit,
    Committed,
    CommittedWithError,
    PartiallyCommitted,
    CancelledAfterCommit,
    DeadlineAfterCommit,
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
    /// Backward-compatible projection of `durable_outcome.committed`.
    pub committed: bool,
    pub durable_outcome: DurableOutcome,
    pub terminal_error: Option<QueryTerminalError>,
    pub serialization_outcome: SerializationOutcome,
    pub outcome_unknown: bool,
    terminal_state_override: Option<QueryTerminalState>,
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

impl QueryStatus {
    pub fn terminal_state(&self) -> Option<QueryTerminalState> {
        if !self.phase.is_terminal() {
            return None;
        }
        if let Some(terminal_state) = self.terminal_state_override {
            return Some(terminal_state);
        }
        if !self.durable_outcome.committed {
            return Some(match (self.phase, self.cancellation_reason) {
                (SqlQueryPhase::Completed, _) => QueryTerminalState::Completed,
                (SqlQueryPhase::Cancelled, CancellationReason::Deadline) => {
                    QueryTerminalState::DeadlineBeforeCommit
                }
                (SqlQueryPhase::Cancelled, _) => QueryTerminalState::CancelledBeforeCommit,
                _ => QueryTerminalState::FailedBeforeCommit,
            });
        }
        Some(match (self.phase, self.cancellation_reason) {
            (SqlQueryPhase::Completed, _) => QueryTerminalState::Committed,
            (SqlQueryPhase::Cancelled, CancellationReason::Deadline) => {
                QueryTerminalState::DeadlineAfterCommit
            }
            (SqlQueryPhase::Cancelled, _) => QueryTerminalState::CancelledAfterCommit,
            _ if self
                .durable_outcome
                .last_commit_statement_index
                .is_some_and(|index| index < self.statement_index) =>
            {
                QueryTerminalState::PartiallyCommitted
            }
            _ => QueryTerminalState::CommittedWithError,
        })
    }
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
    outcome: Mutex<QueryOutcomeState>,
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
        let outcome = self.outcome.lock().clone();
        let phase = outcome.phase_override.unwrap_or_else(|| self.phase());
        let mut trace = self.trace.lock().clone();
        trace.transition(phase);
        QueryStatus {
            query_id: self.id,
            owner: self.owner.clone(),
            session_id: self.session_id.clone(),
            phase,
            started_at: self.started_at,
            deadline: self.deadline,
            operation: self.operation.lock().clone(),
            sql_fingerprint: *self.sql_fingerprint.lock(),
            cancellation_reason: outcome
                .cancellation_reason_override
                .unwrap_or_else(|| self.control.reason()),
            committed: outcome.durable.committed,
            durable_outcome: outcome.durable,
            terminal_error: outcome.terminal_error,
            serialization_outcome: outcome.serialization,
            outcome_unknown: outcome.terminal_state_override
                == Some(QueryTerminalState::OutcomeUnknown),
            terminal_state_override: outcome.terminal_state_override,
            completed_statements: self.completed_statements.load(Ordering::Acquire),
            statement_index: self.statement_index.load(Ordering::Acquire),
            cancel_requested_at: *self.cancel_requested_at.lock(),
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
        self.trace.lock().transition(phase);
    }
}

#[derive(Debug, Clone)]
struct FinishedQuery {
    status: QueryStatus,
    finished_at: Instant,
    approximate_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct ReservedQueryId {
    query_id: QueryId,
    finished_at: Instant,
}

#[derive(Debug, Default)]
struct RegistryState {
    active: HashMap<QueryId, Arc<RegisteredQuery>>,
    finished: VecDeque<FinishedQuery>,
    finished_bytes: usize,
    reserved_ids: HashMap<QueryId, (Option<String>, Option<String>)>,
    reservations: VecDeque<ReservedQueryId>,
}

#[derive(Debug)]
pub struct SqlQueryRegistry {
    state: Mutex<RegistryState>,
    max_active: usize,
    max_finished: usize,
    max_finished_bytes: usize,
    max_tracked_ids: usize,
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
        let compact_reservations = max_finished
            .max(max_finished_bytes / APPROXIMATE_RESERVED_ID_BYTES)
            .max(1);
        Self {
            state: Mutex::new(RegistryState::default()),
            max_active: max_active.max(1),
            max_finished,
            max_finished_bytes,
            max_tracked_ids: max_active.max(1).saturating_add(compact_reservations),
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
        let deadline_base = Instant::now();
        let deadline = options
            .timeout
            .map(|timeout| {
                deadline_base.checked_add(timeout).ok_or_else(|| {
                    MongrelQueryError::Core(mongreldb_core::MongrelError::InvalidArgument(
                        "query timeout exceeds the monotonic clock range".into(),
                    ))
                })
            })
            .transpose()?;
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
            outcome: Mutex::new(QueryOutcomeState::default()),
            completed_statements: AtomicUsize::new(0),
            statement_index: AtomicUsize::new(0),
            cancel_requested_at: Mutex::new(None),
            trace: Mutex::new(QueryTrace::new(started_at)),
        });
        let mut state = self.state.lock();
        self.prune_locked(&mut state);
        if state.active.contains_key(&id) || state.reserved_ids.contains_key(&id) {
            return Err(MongrelQueryError::QueryIdConflict { query_id: id });
        }
        if state.active.len() >= self.max_active
            || state.active.len().saturating_add(state.reserved_ids.len()) >= self.max_tracked_ids
        {
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
            let mut state = self.state.lock();
            self.prune_locked(&mut state);
            if let Some(query) = state.active.get(&query_id) {
                Some(Arc::clone(query))
            } else if state.reserved_ids.contains_key(&query_id) {
                return CancelOutcome::AlreadyFinished;
            } else {
                return CancelOutcome::NotFound;
            }
        };
        match query {
            Some(query) => query.request_cancel(CancellationReason::ClientRequest),
            None => CancelOutcome::NotFound,
        }
    }

    pub fn status(&self, query_id: QueryId) -> Option<QueryStatus> {
        let mut state = self.state.lock();
        self.prune_locked(&mut state);
        state
            .active
            .get(&query_id)
            .map(|query| query.status())
            .or_else(|| {
                state
                    .finished
                    .iter()
                    .rev()
                    .find(|finished| finished.status.query_id == query_id)
                    .map(|finished| finished.status.clone())
            })
    }

    /// Return bounded owner/session metadata after the detailed status was evicted.
    pub fn compact_finished_identity(
        &self,
        query_id: QueryId,
    ) -> Option<(Option<String>, Option<String>)> {
        let mut state = self.state.lock();
        self.prune_locked(&mut state);
        state.reserved_ids.get(&query_id).cloned()
    }

    pub fn cancel_session(&self, session_id: &str, reason: CancellationReason) -> usize {
        let queries = {
            let state = self.state.lock();
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
        self.state.lock().active.len()
    }

    pub fn active_statuses(&self) -> Vec<QueryStatus> {
        self.state
            .lock()
            .active
            .values()
            .map(|query| query.status())
            .collect()
    }

    pub fn active_for_session(&self, session_id: &str) -> usize {
        self.state
            .lock()
            .active
            .values()
            .filter(|query| query.session_id.as_deref() == Some(session_id))
            .count()
    }

    pub fn queued_count(&self) -> usize {
        self.state
            .lock()
            .active
            .values()
            .filter(|query| query.phase() == SqlQueryPhase::Queued)
            .count()
    }

    pub fn entry_count(&self) -> usize {
        let mut state = self.state.lock();
        self.prune_locked(&mut state);
        state.active.len() + state.finished.len()
    }

    pub fn approximate_bytes(&self) -> usize {
        let mut state = self.state.lock();
        self.prune_locked(&mut state);
        state.finished_bytes
            + state
                .active
                .len()
                .saturating_mul(std::mem::size_of::<RegisteredQuery>())
            + state
                .reserved_ids
                .len()
                .saturating_mul(APPROXIMATE_RESERVED_ID_BYTES)
    }

    pub fn finished_count(&self) -> usize {
        let mut state = self.state.lock();
        self.prune_locked(&mut state);
        state.finished.len()
    }

    fn finish(&self, query: &Arc<RegisteredQuery>) {
        debug_assert!(query.phase().is_terminal());
        let status = query.status();
        let approximate_bytes = std::mem::size_of::<FinishedQuery>()
            + status.owner.as_ref().map_or(0, String::len)
            + status.session_id.as_ref().map_or(0, String::len)
            + status.operation.len()
            + status
                .terminal_error
                .as_ref()
                .map_or(0, |error| error.code.len());
        let mut state = self.state.lock();
        if state.active.remove(&query.id).is_none() {
            return;
        }
        let finished_at = Instant::now();
        state
            .reserved_ids
            .insert(query.id, (status.owner.clone(), status.session_id.clone()));
        state.reservations.push_back(ReservedQueryId {
            query_id: query.id,
            finished_at,
        });
        if self.max_finished > 0 && self.max_finished_bytes > 0 {
            state.finished_bytes = state.finished_bytes.saturating_add(approximate_bytes);
            state.finished.push_back(FinishedQuery {
                status,
                finished_at,
                approximate_bytes,
            });
        }
        self.prune_locked(&mut state);
    }

    fn prune_locked(&self, state: &mut RegistryState) {
        let now = Instant::now();
        while state.reservations.front().is_some_and(|entry| {
            now.saturating_duration_since(entry.finished_at) >= self.finished_ttl
        }) {
            if let Some(entry) = state.reservations.pop_front() {
                state.reserved_ids.remove(&entry.query_id);
            }
        }
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
                        self.control.cancel(reason);
                        *self.cancel_requested_at.lock() = Some(Instant::now());
                        let mut trace = self.trace.lock();
                        trace.transition(phase);
                        trace.cancel_requested_phase = Some(phase);
                        if trace.commit_fence_outcome == CommitFenceOutcome::NotReached {
                            trace.commit_fence_outcome = CommitFenceOutcome::CancelWon;
                        }
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

enum TerminalFailure {
    Error {
        code: String,
        category: QueryTerminalErrorCategory,
    },
    Serialization(String),
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
        let operation = sql
            .split_whitespace()
            .next()
            .unwrap_or("UNKNOWN")
            .chars()
            .take(64)
            .collect::<String>()
            .to_ascii_uppercase();
        *self.query.operation.lock() = operation;
        *self.query.sql_fingerprint.lock() = crate::normalized_sql_fingerprint(sql);
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
            self.query.trace.lock().commit_fence_outcome = CommitFenceOutcome::CancelWon;
            return Err(error);
        }
        self.transition(SqlQueryPhase::Executing, SqlQueryPhase::CommitCritical)?;
        self.query.trace.lock().commit_fence_outcome = CommitFenceOutcome::CommitWon;
        Ok(())
    }

    pub fn exit_commit_critical(&self) -> Result<()> {
        self.transition(SqlQueryPhase::CommitCritical, SqlQueryPhase::Executing)
    }

    pub fn begin_serialization(&self) -> Result<()> {
        self.checkpoint()?;
        match self.phase() {
            SqlQueryPhase::Executing => {
                self.transition(SqlQueryPhase::Executing, SqlQueryPhase::Serializing)
            }
            SqlQueryPhase::Streaming => {
                self.transition(SqlQueryPhase::Streaming, SqlQueryPhase::Serializing)
            }
            phase => Err(MongrelQueryError::InvalidQueryState(format!(
                "query {} cannot serialize from {phase:?}",
                self.id()
            ))),
        }?;
        self.query.outcome.lock().serialization = SerializationOutcome::InProgress;
        Ok(())
    }

    /// Records one statement whose effects are durably published.
    pub fn record_commit(&self, statement_index: usize, epoch: u64) {
        self.query
            .outcome
            .lock()
            .durable
            .record_commit(statement_index, Some(epoch));
    }

    pub fn durable_outcome(&self) -> DurableOutcome {
        self.query.outcome.lock().durable.clone()
    }

    pub fn commit_outcome_error(&self, message: impl Into<String>) -> MongrelQueryError {
        let durable = self.durable_outcome();
        MongrelQueryError::CommitOutcome {
            query_id: self.id(),
            committed: durable.committed,
            committed_statements: durable.committed_statements,
            last_commit_epoch: durable.last_commit_epoch,
            first_commit_statement_index: durable.first_commit_statement_index,
            last_commit_statement_index: durable.last_commit_statement_index,
            completed_statements: self.query.completed_statements.load(Ordering::Acquire),
            statement_index: self.query.statement_index.load(Ordering::Acquire),
            message: message.into(),
        }
    }

    pub fn result_limit_error(&self, message: impl Into<String>) -> MongrelQueryError {
        let durable = self.durable_outcome();
        MongrelQueryError::ResultLimitExceeded {
            query_id: self.id(),
            committed: durable.committed,
            committed_statements: durable.committed_statements,
            last_commit_epoch: durable.last_commit_epoch,
            first_commit_statement_index: durable.first_commit_statement_index,
            last_commit_statement_index: durable.last_commit_statement_index,
            completed_statements: self.query.completed_statements.load(Ordering::Acquire),
            statement_index: self.query.statement_index.load(Ordering::Acquire),
            message: message.into(),
        }
    }

    /// A fenced mutation failed without an exact durable receipt. Never claim
    /// `committed=false`: storage may have changed before the error surfaced.
    pub fn outcome_unknown_error(&self, message: impl Into<String>) -> MongrelQueryError {
        let message = message.into();
        self.mark_outcome_unknown();
        MongrelQueryError::OutcomeUnknown {
            query_id: self.id(),
            message,
        }
    }

    /// Restore a terminal receipt onto a newly registered idempotent replay so
    /// status polling for the replay query ID reports the same durable result.
    #[allow(clippy::too_many_arguments)]
    pub fn restore_replayed_outcome(
        &self,
        durable: DurableOutcome,
        completed_statements: usize,
        statement_index: usize,
        serialization: SerializationOutcome,
        terminal_error: Option<QueryTerminalError>,
        terminal_state: QueryTerminalState,
        cancellation_reason: CancellationReason,
        phase: SqlQueryPhase,
    ) {
        let mut outcome = self.query.outcome.lock();
        outcome.durable = durable;
        outcome.serialization = serialization;
        outcome.terminal_error = terminal_error;
        outcome.terminal_state_override = Some(terminal_state);
        outcome.cancellation_reason_override = Some(cancellation_reason);
        outcome.phase_override = Some(phase);
        self.query
            .completed_statements
            .store(completed_statements, Ordering::Release);
        self.query
            .statement_index
            .store(statement_index, Ordering::Release);
    }

    /// The daemon found a durable idempotency intent without a receipt. The
    /// previous write may have committed, so `committed=false` is not known.
    pub fn mark_outcome_unknown(&self) {
        let mut outcome = self.query.outcome.lock();
        outcome.terminal_state_override = Some(QueryTerminalState::OutcomeUnknown);
        outcome.terminal_error = Some(QueryTerminalError {
            code: "QUERY_OUTCOME_UNKNOWN".into(),
            category: QueryTerminalErrorCategory::Execution,
        });
    }

    pub fn record_terminal_error(
        &self,
        code: impl Into<String>,
        category: QueryTerminalErrorCategory,
    ) {
        Self::set_terminal_error(&mut self.query.outcome.lock(), code.into(), category);
    }

    pub fn record_serialization_failure(&self, code: impl Into<String>) {
        let mut outcome = self.query.outcome.lock();
        Self::set_serialization_failure(&mut outcome, code.into());
    }

    fn set_terminal_error(
        outcome: &mut QueryOutcomeState,
        code: String,
        category: QueryTerminalErrorCategory,
    ) {
        outcome.terminal_error = Some(QueryTerminalError { code, category });
    }

    fn set_serialization_failure(outcome: &mut QueryOutcomeState, code: String) {
        let code = if outcome.durable.committed && code.starts_with("SERIALIZATION_") {
            "SERIALIZATION_FAILED_AFTER_COMMIT".into()
        } else {
            code
        };
        outcome.serialization = SerializationOutcome::Failed;
        outcome.terminal_error = Some(QueryTerminalError {
            code,
            category: QueryTerminalErrorCategory::Serialization,
        });
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
        let durable = self.durable_outcome();
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
                        committed: durable.committed,
                        committed_statements: durable.committed_statements,
                        last_commit_epoch: durable.last_commit_epoch,
                        first_commit_statement_index: durable.first_commit_statement_index,
                        last_commit_statement_index: durable.last_commit_statement_index,
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
            let mut trace = self.query.trace.lock();
            trace.cancel_observed_phase = trace.cancel_requested_phase.or(Some(self.phase()));
        }
        result
    }

    pub fn complete(&self) -> Result<()> {
        self.try_complete()
    }

    pub fn try_complete(&self) -> Result<()> {
        if self.phase() == SqlQueryPhase::Completed {
            self.finish(SqlQueryPhase::Completed, None);
            return Ok(());
        }
        if let Err(error) = self.checkpoint() {
            self.fail();
            return Err(error);
        }
        let phase = self.finish(SqlQueryPhase::Completed, None);
        match phase {
            SqlQueryPhase::Completed => Ok(()),
            SqlQueryPhase::Cancelled => Err(self.cancellation_error()),
            other => Err(MongrelQueryError::InvalidQueryState(format!(
                "query {} completed from terminal phase {other:?}",
                self.id()
            ))),
        }
    }

    pub fn fail(&self) {
        self.finish(SqlQueryPhase::Failed, None);
    }

    pub fn fail_result_limit(&self) {
        self.fail_with_error(
            "RESULT_LIMIT_EXCEEDED",
            QueryTerminalErrorCategory::ResultLimit,
        );
    }

    pub fn fail_with_error(&self, code: impl Into<String>, category: QueryTerminalErrorCategory) {
        self.finish(
            SqlQueryPhase::Failed,
            Some(TerminalFailure::Error {
                code: code.into(),
                category,
            }),
        );
    }

    pub fn fail_serialization(&self) {
        self.finish(
            SqlQueryPhase::Failed,
            Some(TerminalFailure::Serialization(
                "SERIALIZATION_FAILED".into(),
            )),
        );
    }

    fn finish(
        &self,
        requested: SqlQueryPhase,
        mut failure: Option<TerminalFailure>,
    ) -> SqlQueryPhase {
        debug_assert!(matches!(
            requested,
            SqlQueryPhase::Completed | SqlQueryPhase::Failed
        ));
        let explicit_failure = failure.is_some();
        let mut outcome = self.query.outcome.lock();
        let phase = loop {
            let current = self.phase();
            if current.is_terminal() {
                break current;
            }
            let terminal = if current == SqlQueryPhase::Cancelling
                || (!explicit_failure && self.query.control.is_cancelled())
            {
                while !self.query.control.is_cancelled() {
                    std::hint::spin_loop();
                }
                SqlQueryPhase::Cancelled
            } else {
                requested
            };
            if self
                .query
                .phase
                .compare_exchange(
                    current as u8,
                    terminal as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.query.record_transition(current);
                if terminal == SqlQueryPhase::Cancelled && outcome.phase_override.is_some() {
                    outcome.phase_override = None;
                    outcome.terminal_state_override = None;
                    outcome.cancellation_reason_override = None;
                    outcome.terminal_error = None;
                    outcome.serialization = SerializationOutcome::Failed;
                }
                if terminal == SqlQueryPhase::Failed {
                    match failure.take() {
                        Some(TerminalFailure::Error { code, category }) => {
                            Self::set_terminal_error(&mut outcome, code, category);
                        }
                        Some(TerminalFailure::Serialization(code)) => {
                            Self::set_serialization_failure(&mut outcome, code);
                        }
                        None => {}
                    }
                }
                Self::finalize_outcome(&mut outcome, terminal, self.query.control.reason());
                break terminal;
            }
        };
        Self::finalize_outcome(&mut outcome, phase, self.query.control.reason());
        drop(outcome);
        if let Some(registry) = self.registry.upgrade() {
            registry.finish(&self.query);
        }
        phase
    }

    fn finalize_outcome(
        outcome: &mut QueryOutcomeState,
        phase: SqlQueryPhase,
        reason: CancellationReason,
    ) {
        match (phase, outcome.serialization) {
            (SqlQueryPhase::Completed, SerializationOutcome::InProgress) => {
                outcome.serialization = SerializationOutcome::Succeeded;
            }
            (
                SqlQueryPhase::Failed | SqlQueryPhase::Cancelled,
                SerializationOutcome::InProgress,
            ) => {
                outcome.serialization = SerializationOutcome::Failed;
            }
            _ => {}
        }
        if phase == SqlQueryPhase::Completed || outcome.terminal_error.is_some() {
            return;
        }
        let committed = outcome.durable.committed;
        let (code, category) = match reason {
            CancellationReason::Deadline => (
                if committed {
                    "DEADLINE_AFTER_COMMIT"
                } else {
                    "DEADLINE_EXCEEDED"
                },
                QueryTerminalErrorCategory::Deadline,
            ),
            CancellationReason::None if outcome.serialization == SerializationOutcome::Failed => (
                if committed {
                    "SERIALIZATION_FAILED_AFTER_COMMIT"
                } else {
                    "SERIALIZATION_FAILED"
                },
                QueryTerminalErrorCategory::Serialization,
            ),
            CancellationReason::None => ("QUERY_FAILED", QueryTerminalErrorCategory::Execution),
            _ => (
                if committed {
                    "QUERY_CANCELLED_AFTER_COMMIT"
                } else {
                    "QUERY_CANCELLED"
                },
                QueryTerminalErrorCategory::Cancellation,
            ),
        };
        outcome.terminal_error = Some(QueryTerminalError {
            code: code.into(),
            category,
        });
    }

    fn cancellation_error(&self) -> MongrelQueryError {
        let durable = self.durable_outcome();
        let mut reason = self.query.control.reason();
        while reason == CancellationReason::None && self.query.phase() == SqlQueryPhase::Cancelling
        {
            std::thread::yield_now();
            reason = self.query.control.reason();
        }
        match reason {
            CancellationReason::Deadline => MongrelQueryError::DeadlineExceeded {
                query_id: self.id(),
                timeout_ms: self.query.deadline.map(|deadline| {
                    deadline
                        .saturating_duration_since(self.query.started_at)
                        .as_millis()
                        .min(u128::from(u64::MAX)) as u64
                }),
                committed: durable.committed,
                committed_statements: durable.committed_statements,
                last_commit_epoch: durable.last_commit_epoch,
                first_commit_statement_index: durable.first_commit_statement_index,
                last_commit_statement_index: durable.last_commit_statement_index,
                completed_statements: self.query.completed_statements.load(Ordering::Acquire),
                cancelled_statement_index: self.query.statement_index.load(Ordering::Acquire),
            },
            reason => MongrelQueryError::QueryCancelled {
                query_id: self.id(),
                reason,
                committed: durable.committed,
                committed_statements: durable.committed_statements,
                last_commit_epoch: durable.last_commit_epoch,
                first_commit_statement_index: durable.first_commit_statement_index,
                last_commit_statement_index: durable.last_commit_statement_index,
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

    pub fn complete(self) -> Result<()> {
        self.try_complete()
    }

    pub fn try_complete(mut self) -> Result<()> {
        if let Some(query) = self.query.take() {
            query.try_complete()
        } else {
            Ok(())
        }
    }

    pub fn fail(mut self) {
        if let Some(query) = self.query.take() {
            query.fail();
        }
    }

    pub fn fail_result_limit(mut self) {
        if let Some(query) = self.query.take() {
            query.fail_result_limit();
        }
    }

    pub fn fail_with_error(
        mut self,
        code: impl Into<String>,
        category: QueryTerminalErrorCategory,
    ) {
        if let Some(query) = self.query.take() {
            query.fail_with_error(code, category);
        }
    }

    pub fn fail_serialization(mut self) {
        if let Some(query) = self.query.take() {
            query.fail_serialization();
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
    fn unrepresentable_timeout_is_rejected_without_panicking() {
        let registry = Arc::new(SqlQueryRegistry::default());
        assert!(matches!(
            registry.register(SqlQueryOptions {
                timeout: Some(Duration::MAX),
                ..SqlQueryOptions::default()
            }),
            Err(MongrelQueryError::Core(
                mongreldb_core::MongrelError::InvalidArgument(_)
            ))
        ));
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn active_and_retained_query_ids_are_rejected() {
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
        query.complete().unwrap();
        assert_eq!(registry.active_count(), 0);
        assert_eq!(registry.finished_count(), 1);
        assert!(matches!(
            registry.register(SqlQueryOptions {
                query_id: Some(id),
                ..SqlQueryOptions::default()
            }),
            Err(MongrelQueryError::QueryIdConflict { .. })
        ));
        assert_eq!(registry.cancel(id), CancelOutcome::AlreadyFinished);
    }

    #[test]
    fn query_id_can_be_reused_after_its_tombstone_expires() {
        let registry = Arc::new(SqlQueryRegistry::new(1, 1, 1024, Duration::ZERO));
        let id = QueryId::random().unwrap();
        registry
            .register(SqlQueryOptions {
                query_id: Some(id),
                ..SqlQueryOptions::default()
            })
            .unwrap()
            .complete()
            .unwrap();

        let replacement = registry
            .register(SqlQueryOptions {
                query_id: Some(id),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        assert_eq!(registry.status(id).unwrap().phase, SqlQueryPhase::Queued);
        replacement.complete().unwrap();
    }

    #[test]
    fn status_capacity_eviction_does_not_release_query_id() {
        for (max_finished, max_finished_bytes) in [(1, usize::MAX), (10, 1)] {
            let owner = "o".repeat(MAX_METADATA_BYTES);
            let session_id = "s".repeat(MAX_METADATA_BYTES);
            let registry = Arc::new(SqlQueryRegistry::new(
                1,
                max_finished,
                max_finished_bytes,
                Duration::from_secs(60),
            ));
            let first_id = QueryId::random().unwrap();
            registry
                .register(SqlQueryOptions {
                    query_id: Some(first_id),
                    owner: Some(owner.clone()),
                    session_id: Some(session_id.clone()),
                    ..SqlQueryOptions::default()
                })
                .unwrap()
                .complete()
                .unwrap();
            registry
                .register(SqlQueryOptions::default())
                .unwrap()
                .complete()
                .unwrap();

            assert!(registry.status(first_id).is_none());
            assert_eq!(
                registry.compact_finished_identity(first_id),
                Some((Some(owner), Some(session_id)))
            );
            assert_eq!(registry.cancel(first_id), CancelOutcome::AlreadyFinished);
            assert!(matches!(
                registry.register(SqlQueryOptions {
                    query_id: Some(first_id),
                    ..SqlQueryOptions::default()
                }),
                Err(MongrelQueryError::QueryIdConflict { .. })
            ));
        }
    }

    #[test]
    fn compact_id_reservations_fail_closed_at_bounded_capacity() {
        let registry = Arc::new(SqlQueryRegistry::new(1, 0, 0, Duration::from_secs(60)));
        let first_id = QueryId::random().unwrap();
        registry
            .register(SqlQueryOptions {
                query_id: Some(first_id),
                ..SqlQueryOptions::default()
            })
            .unwrap()
            .complete()
            .unwrap();
        registry
            .register(SqlQueryOptions::default())
            .unwrap()
            .complete()
            .unwrap();

        assert_eq!(registry.finished_count(), 0);
        assert_eq!(registry.cancel(first_id), CancelOutcome::AlreadyFinished);
        assert!(matches!(
            registry.register(SqlQueryOptions::default()),
            Err(MongrelQueryError::QueryRegistryFull)
        ));
        assert!(registry.approximate_bytes() <= 2 * APPROXIMATE_RESERVED_ID_BYTES);
    }

    #[test]
    fn compact_identity_accounting_covers_maximum_metadata() {
        let registry = Arc::new(SqlQueryRegistry::new(1, 0, 0, Duration::from_secs(60)));
        let owner = "o".repeat(MAX_METADATA_BYTES);
        let session_id = "s".repeat(MAX_METADATA_BYTES);
        let query_id = QueryId::random().unwrap();
        registry
            .register(SqlQueryOptions {
                query_id: Some(query_id),
                owner: Some(owner.clone()),
                session_id: Some(session_id.clone()),
                ..SqlQueryOptions::default()
            })
            .unwrap()
            .complete()
            .unwrap();

        assert_eq!(
            registry.compact_finished_identity(query_id),
            Some((Some(owner), Some(session_id)))
        );
        assert!(registry.approximate_bytes() <= APPROXIMATE_RESERVED_ID_BYTES);
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
                query.record_commit(0, 7);
                query.complete().unwrap();
                let status = registry.status(query.id()).unwrap();
                assert!(status.committed);
                assert_eq!(status.commit_fence_outcome, CommitFenceOutcome::CommitWon);
            }
        }
    }

    #[test]
    fn cancel_and_terminal_completion_have_one_winner() {
        for _ in 0..100 {
            let registry = Arc::new(SqlQueryRegistry::default());
            let query = registry.register(SqlQueryOptions::default()).unwrap();
            query
                .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
                .unwrap();
            query.begin_serialization().unwrap();

            let barrier = Arc::new(std::sync::Barrier::new(3));
            let cancel_query = query.clone();
            let cancel_barrier = Arc::clone(&barrier);
            let cancel = std::thread::spawn(move || {
                cancel_barrier.wait();
                cancel_query.request_cancel(CancellationReason::ClientRequest)
            });
            let complete_query = query.clone();
            let complete_barrier = Arc::clone(&barrier);
            let complete = std::thread::spawn(move || {
                complete_barrier.wait();
                complete_query.try_complete()
            });
            barrier.wait();

            let cancel = cancel.join().unwrap();
            let complete = complete.join().unwrap();
            let status = registry.status(query.id()).unwrap();
            match cancel {
                CancelOutcome::Accepted => {
                    assert!(matches!(
                        complete,
                        Err(MongrelQueryError::QueryCancelled { .. })
                    ));
                    assert_eq!(status.phase, SqlQueryPhase::Cancelled);
                }
                CancelOutcome::AlreadyFinished => {
                    complete.unwrap();
                    assert_eq!(status.phase, SqlQueryPhase::Completed);
                }
                other => panic!("unexpected cancel outcome {other:?}"),
            }
        }
    }

    #[test]
    fn externally_claimed_completion_is_finalized_and_retained() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_serialization().unwrap();
        query
            .transition(SqlQueryPhase::Serializing, SqlQueryPhase::Completed)
            .unwrap();

        query.try_complete().unwrap();

        assert_eq!(registry.active_count(), 0);
        let status = registry.status(query.id()).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Completed);
        assert_eq!(
            status.serialization_outcome,
            SerializationOutcome::Succeeded
        );
    }

    #[test]
    fn zero_batch_completion_checks_expired_deadline() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry
            .register(SqlQueryOptions {
                timeout: Some(Duration::from_millis(1)),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_serialization().unwrap();
        std::thread::sleep(Duration::from_millis(5));

        assert!(matches!(
            query.try_complete(),
            Err(MongrelQueryError::DeadlineExceeded { .. })
        ));
        let status = registry.status(query.id()).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Cancelled);
        assert_eq!(
            status.terminal_state(),
            Some(QueryTerminalState::DeadlineBeforeCommit)
        );
    }

    #[test]
    fn parent_cancellation_wins_terminal_completion() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let parent = ExecutionControl::new(None);
        let query = registry
            .register(SqlQueryOptions {
                parent_control: Some(parent.clone()),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_serialization().unwrap();
        parent.cancel(CancellationReason::SessionClosed);

        assert!(matches!(
            query.try_complete(),
            Err(MongrelQueryError::QueryCancelled {
                reason: CancellationReason::SessionClosed,
                ..
            })
        ));
        assert_eq!(
            registry.status(query.id()).unwrap().phase,
            SqlQueryPhase::Cancelled
        );
    }

    #[test]
    fn explicit_serialization_failure_beats_passive_parent_cancel() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let parent = ExecutionControl::new(None);
        let query = registry
            .register(SqlQueryOptions {
                parent_control: Some(parent.clone()),
                ..SqlQueryOptions::default()
            })
            .unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_serialization().unwrap();
        parent.cancel(CancellationReason::SessionClosed);
        query.fail_serialization();

        let status = registry.status(query.id()).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Failed);
        assert_eq!(status.terminal_error.unwrap().code, "SERIALIZATION_FAILED");
    }

    #[test]
    fn serialization_failure_after_commit_keeps_exact_durable_outcome() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_statement(2);
        query.enter_commit_critical().unwrap();
        query.record_commit(2, 73);
        query.exit_commit_critical().unwrap();
        query.begin_serialization().unwrap();
        query.fail_serialization();

        let status = registry.status(query.id()).unwrap();
        assert_eq!(status.phase, SqlQueryPhase::Failed);
        assert_eq!(status.durable_outcome.last_commit_epoch, Some(73));
        assert_eq!(status.durable_outcome.committed_statements, 1);
        assert_eq!(
            status.terminal_error.unwrap().code,
            "SERIALIZATION_FAILED_AFTER_COMMIT"
        );
        assert!(matches!(
            query.commit_outcome_error("encode failed"),
            MongrelQueryError::CommitOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(73),
                first_commit_statement_index: Some(2),
                last_commit_statement_index: Some(2),
                statement_index: 2,
                ..
            }
        ));
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

    #[test]
    fn sql_operation_metadata_is_bounded() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let id = query.id();
        query.set_sql_metadata(&format!("{} value", "x".repeat(4096)));
        drop(RegisteredQueryGuard::new(query));
        assert_eq!(registry.status(id).unwrap().operation.len(), 64);
    }

    #[test]
    fn tombstone_keeps_durable_and_terminal_outcomes() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let id = query.id();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_statement(0);
        query.enter_commit_critical().unwrap();
        query.record_commit(0, 41);
        query.record_commit(0, 41);
        query.exit_commit_critical().unwrap();
        query.complete_statement(0);
        query.begin_statement(1);
        query.begin_serialization().unwrap();
        let active_outcome = query.status().durable_outcome;

        assert_eq!(
            query.request_cancel(CancellationReason::ClientRequest),
            CancelOutcome::Accepted
        );
        query.fail();

        let status = registry.status(id).unwrap();
        assert_eq!(status.durable_outcome, active_outcome);
        assert_eq!(
            status.durable_outcome,
            DurableOutcome {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(41),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
            }
        );
        assert_eq!(status.serialization_outcome, SerializationOutcome::Failed);
        assert_eq!(status.commit_fence_outcome, CommitFenceOutcome::CommitWon);
        assert_eq!(
            status.terminal_error,
            Some(QueryTerminalError {
                code: "QUERY_CANCELLED_AFTER_COMMIT".into(),
                category: QueryTerminalErrorCategory::Cancellation,
            })
        );
        assert_eq!(
            status.terminal_state(),
            Some(QueryTerminalState::CancelledAfterCommit)
        );
    }

    #[test]
    fn cancellation_error_carries_durable_receipt() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        query
            .transition(SqlQueryPhase::Queued, SqlQueryPhase::Executing)
            .unwrap();
        query.begin_statement(0);
        query.enter_commit_critical().unwrap();
        query.record_commit(0, 73);
        query.exit_commit_critical().unwrap();
        query.complete_statement(0);
        query.begin_statement(1);
        query.begin_serialization().unwrap();
        assert_eq!(
            query.request_cancel(CancellationReason::ClientRequest),
            CancelOutcome::Accepted
        );

        assert!(matches!(
            query.checkpoint(),
            Err(MongrelQueryError::QueryCancelled {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: Some(73),
                first_commit_statement_index: Some(0),
                last_commit_statement_index: Some(0),
                completed_statements: 1,
                cancelled_statement_index: 1,
                ..
            })
        ));
        query.fail();
    }

    #[test]
    fn explicit_terminal_error_category_survives_tombstone() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let id = query.id();
        query.record_terminal_error(
            "RESULT_LIMIT_EXCEEDED",
            QueryTerminalErrorCategory::ResultLimit,
        );
        query.fail();

        let status = registry.status(id).unwrap();
        assert_eq!(
            status.terminal_error,
            Some(QueryTerminalError {
                code: "RESULT_LIMIT_EXCEEDED".into(),
                category: QueryTerminalErrorCategory::ResultLimit,
            })
        );
        assert_eq!(
            status.terminal_state(),
            Some(QueryTerminalState::FailedBeforeCommit)
        );
    }

    #[test]
    fn serialization_failure_code_records_commit_outcome() {
        let registry = Arc::new(SqlQueryRegistry::default());
        let query = registry.register(SqlQueryOptions::default()).unwrap();
        let id = query.id();
        query.record_commit(0, 7);
        query.record_serialization_failure("SERIALIZATION_FAILED");
        query.fail();

        let status = registry.status(id).unwrap();
        assert_eq!(
            status.terminal_error.as_ref().unwrap().code,
            "SERIALIZATION_FAILED_AFTER_COMMIT"
        );
        assert_eq!(
            status.terminal_state(),
            Some(QueryTerminalState::CommittedWithError)
        );
    }
}
