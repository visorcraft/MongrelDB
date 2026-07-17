//! Protocol service definitions (spec section 10.4, S1D-003).
//!
//! The seven services below are the server's contract with its protocol
//! adapters (native RPC, HTTP/JSON, Kit, MySQL wire): adapters translate
//! wire requests into the canonical model of [`crate::request`] and call
//! these traits; the server wave implements them.
//!
//! # Errors
//!
//! Every method returns [`CategoryError`]
//! ([`mongreldb_types::errors::CategoryError`]), the structural taxonomy of
//! spec section 9.7 that every language binding maps. Programmatic handling
//! keys off the category (or its stable code), never the message.
//!
//! # Async shape: object-safe boxed futures
//!
//! The traits use hand-written boxed futures ([`BoxFuture`]) instead of
//! native `async fn` in traits. Native async-fn-in-trait (stable, and usable
//! on this workspace's Rust 1.88) desugars to RPITIT, which is NOT
//! object-safe: `Arc<dyn QueryService>` would be impossible, forcing every
//! adapter to monomorphize around concrete service types. The adapters
//! dispatch services dynamically, so object safety is required — and this
//! crate's dependency set is frozen, so the `async-trait` crate is not
//! available to bridge the gap. The cost is one heap allocation per call,
//! acceptable at the protocol boundary where a call is already a network
//! request. The same choice gives an object-safe [`ArrowFrameStream`]
//! without a `futures` dependency.

use core::pin::Pin;
use std::future::Future;

use mongreldb_types::errors::CategoryError;
use mongreldb_types::ids::{DatabaseId, QueryId, SchemaVersion, TransactionId};

use crate::prepared::PreparedStatementBinding;
use crate::request::{AuthenticatedIdentity, ExecuteRequest, IsolationLevel, SessionId};
use crate::session::Session;

/// The boxed future every service method returns: object-safe, `Send`, and
/// resolving to `Result<T, CategoryError>`. See the module-level
/// documentation for why this is not native `async fn` in traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, CategoryError>> + Send + 'a>>;

/// Credentials presented at session open.
///
/// Variants are never reordered and discriminants never reused (spec
/// section 4.10); new credential kinds are only appended.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Credentials {
    /// Catalog username + password, verified against the catalog's Argon2id
    /// password hashes.
    Password {
        /// Case-sensitive username.
        username: String,
        /// Cleartext password, verified and immediately discarded; the wire
        /// form is protected by TLS 1.3 (S1D-002).
        password: String,
    },
}

/// The buffered result of a non-streaming [`QueryService::execute`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExecuteResponse {
    /// The query execution that produced this response.
    pub query_id: QueryId,
    /// Rows affected (for DML); zero for queries.
    pub rows_affected: u64,
    /// Result frames: Arrow IPC byte chunks, the buffered form of the
    /// [`ArrowFrameStream`] contract. Empty for commands without a result
    /// set. The protocol crate fixes framing only; real Arrow record-batch
    /// encoding lands in the server wave.
    pub frames: Vec<Vec<u8>>,
}

/// The execution phase of a query (S1D-006).
///
/// Variants are never reordered and discriminants never reused (spec
/// section 4.10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryPhase {
    /// Waiting for admission (queue wait counts toward the deadline).
    Queued,
    /// Parsing and planning.
    Planning,
    /// Executing.
    Executing,
    /// Serializing result frames (counts toward the deadline).
    Serializing,
    /// Finished successfully (durable outcome).
    Completed,
    /// Finished with a failure (durable outcome); see
    /// [`QueryStatus::error`].
    Failed,
    /// Cancelled by the caller or by deadline expiry (durable outcome).
    Cancelled,
}

/// The status of one query execution, as returned by
/// [`QueryService::get_query_status`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueryStatus {
    /// The query this status describes.
    pub query_id: QueryId,
    /// Current (or final) phase.
    pub phase: QueryPhase,
    /// The failure that ended the query, present iff
    /// [`QueryPhase::Failed`].
    pub error: Option<CategoryError>,
}

/// One column of a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    /// Column name.
    pub name: String,
    /// Canonical type name (e.g. `INT64`, `TEXT`).
    pub data_type: String,
    /// Whether the column accepts `NULL`.
    pub nullable: bool,
}

/// The schema of one table, as returned by
/// [`CatalogService::get_schema`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    /// Table name.
    pub table: String,
    /// Current schema version of the table; clients pin prepared statements
    /// to it (S1D-005).
    pub schema_version: SchemaVersion,
    /// Columns in declaration order.
    pub columns: Vec<ColumnSchema>,
}

/// The serving state reported by [`HealthService::status`].
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HealthStatus {
    /// Whether the server is accepting requests.
    pub serving: bool,
    /// Optional human-readable detail (e.g. why the server is not serving).
    pub detail: Option<String>,
}

/// A pull stream of Arrow IPC byte frames, as returned by
/// [`QueryService::execute_stream`].
///
/// Each frame is one Arrow IPC byte chunk carried on the wire as the
/// payload of a [`crate::envelope::ProtocolEnvelope`] (spec section 4.10);
/// the protocol crate fixes the framing contract only — real Arrow
/// record-batch encoding lands in the server wave. `Ok(Some(frame))` yields
/// one chunk, `Ok(None)` ends the stream, and `Err` is terminal (the stream
/// yields nothing further).
pub trait ArrowFrameStream: Send {
    /// Pulls the next frame; see the trait documentation for the contract.
    fn next_frame(&mut self) -> BoxFuture<'_, Option<Vec<u8>>>;
}

impl ArrowFrameStream for std::vec::IntoIter<Vec<u8>> {
    fn next_frame(&mut self) -> BoxFuture<'_, Option<Vec<u8>>> {
        let frame = self.next();
        Box::pin(async move { Ok(frame) })
    }
}

/// Authentication (S1D-003): turns credentials into an
/// [`AuthenticatedIdentity`]. Failures fail closed as
/// [`mongreldb_types::errors::ErrorCategory::Unauthenticated`].
pub trait AuthService: Send + Sync {
    /// Verifies credentials and returns the authenticated identity.
    fn authenticate<'a>(
        &'a self,
        credentials: &'a Credentials,
    ) -> BoxFuture<'a, AuthenticatedIdentity>;
}

/// Session lifecycle (S1D-003, S1D-004).
pub trait SessionService: Send + Sync {
    /// Opens a session for an authenticated principal on a database.
    fn open_session(
        &self,
        principal: AuthenticatedIdentity,
        database_id: DatabaseId,
    ) -> BoxFuture<'_, Session>;

    /// Closes a session, rolling back any active transaction and dropping
    /// its prepared statements.
    fn close_session(&self, session_id: SessionId) -> BoxFuture<'_, ()>;
}

/// Query preparation, execution, streaming, and cancellation (S1D-003).
pub trait QueryService: Send + Sync {
    /// Prepares a SQL statement on a session, returning the binding record
    /// (S1D-005) the executor validates before every execution.
    fn prepare(
        &self,
        session_id: SessionId,
        sql: String,
    ) -> BoxFuture<'_, PreparedStatementBinding>;

    /// Executes a canonical request, buffering the result.
    fn execute(&self, request: ExecuteRequest) -> BoxFuture<'_, ExecuteResponse>;

    /// Executes a canonical request, streaming the result as Arrow IPC byte
    /// frames (see [`ArrowFrameStream`]).
    fn execute_stream(&self, request: ExecuteRequest) -> BoxFuture<'_, Box<dyn ArrowFrameStream>>;

    /// Cancels a running query (S1D-006). Cancelling an unknown query fails;
    /// a finished query keeps its durable outcome (spec section 4.7), which
    /// [`QueryService::get_query_status`] reports.
    fn cancel_query(&self, query_id: QueryId) -> BoxFuture<'_, ()>;

    /// Returns the current status of a query, including its durable outcome
    /// once finished.
    fn get_query_status(&self, query_id: QueryId) -> BoxFuture<'_, QueryStatus>;
}

/// Explicit transaction control on a session (S1D-003).
pub trait TransactionService: Send + Sync {
    /// Begins a transaction at the requested isolation level.
    fn begin(
        &self,
        session_id: SessionId,
        isolation: IsolationLevel,
    ) -> BoxFuture<'_, TransactionId>;

    /// Commits the session's active transaction. Commit failures carry the
    /// transaction categories of the taxonomy (e.g.
    /// [`mongreldb_types::errors::ErrorCategory::TransactionConflict`],
    /// [`mongreldb_types::errors::ErrorCategory::CommitOutcomeUnknown`]); an
    /// ambiguous outcome is only replayed with a durable idempotency key
    /// (spec section 11.7).
    fn commit(&self, session_id: SessionId) -> BoxFuture<'_, ()>;

    /// Rolls back the session's active transaction.
    fn rollback(&self, session_id: SessionId) -> BoxFuture<'_, ()>;
}

/// Catalog reads (S1D-003).
pub trait CatalogService: Send + Sync {
    /// Returns the current schema of one table.
    fn get_schema(&self, database_id: DatabaseId, table: String) -> BoxFuture<'_, TableSchema>;
}

/// Administrative operations (S1D-003). The request's command must be
/// [`crate::request::ExecuteCommand::Admin`]; non-admin principals fail as
/// [`mongreldb_types::errors::ErrorCategory::PermissionDenied`].
pub trait AdminService: Send + Sync {
    /// Executes the admin command of a canonical request.
    fn execute_admin(&self, request: ExecuteRequest) -> BoxFuture<'_, ()>;
}

/// Liveness and readiness (S1D-003).
pub trait HealthService: Send + Sync {
    /// Returns the current serving state.
    fn status(&self) -> BoxFuture<'_, HealthStatus>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{ExecuteCommand, ResultLimits};
    use crate::test_support::{assert_serde_round_trip, block_on};
    use mongreldb_types::errors::ErrorCategory;
    use std::sync::Arc;

    #[test]
    fn dto_serde_round_trips() {
        assert_serde_round_trip(&Credentials::Password {
            username: "alice".to_owned(),
            password: "s3cret".to_owned(),
        });
        assert_serde_round_trip(&ExecuteResponse {
            query_id: QueryId::new_random(),
            rows_affected: 17,
            frames: vec![b"arrow-ipc-bytes".to_vec(), vec![]],
        });
        for phase in [
            QueryPhase::Queued,
            QueryPhase::Planning,
            QueryPhase::Executing,
            QueryPhase::Serializing,
            QueryPhase::Completed,
            QueryPhase::Failed,
            QueryPhase::Cancelled,
        ] {
            assert_serde_round_trip(&phase);
        }
        assert_serde_round_trip(&QueryStatus {
            query_id: QueryId::new_random(),
            phase: QueryPhase::Failed,
            error: Some(CategoryError::new(
                ErrorCategory::DeadlineExceeded,
                "deadline expired during execution",
            )),
        });
        assert_serde_round_trip(&QueryStatus {
            query_id: QueryId::new_random(),
            phase: QueryPhase::Completed,
            error: None,
        });
        assert_serde_round_trip(&ColumnSchema {
            name: "tenant".to_owned(),
            data_type: "INT64".to_owned(),
            nullable: false,
        });
        assert_serde_round_trip(&TableSchema {
            table: "events".to_owned(),
            schema_version: SchemaVersion::new(10),
            columns: vec![
                ColumnSchema {
                    name: "tenant".to_owned(),
                    data_type: "INT64".to_owned(),
                    nullable: false,
                },
                ColumnSchema {
                    name: "payload".to_owned(),
                    data_type: "TEXT".to_owned(),
                    nullable: true,
                },
            ],
        });
        assert_serde_round_trip(&HealthStatus {
            serving: true,
            detail: None,
        });
        assert_serde_round_trip(&HealthStatus {
            serving: false,
            detail: Some("draining".to_owned()),
        });
    }

    /// A stub auth service that always fails closed.
    struct StubAuth;

    impl AuthService for StubAuth {
        fn authenticate<'a>(
            &'a self,
            credentials: &'a Credentials,
        ) -> BoxFuture<'a, AuthenticatedIdentity> {
            Box::pin(async move {
                let Credentials::Password { username, .. } = credentials;
                Err(CategoryError::new(
                    ErrorCategory::Unauthenticated,
                    format!("invalid credentials for {username:?}"),
                ))
            })
        }
    }

    #[test]
    fn category_error_propagates_through_dyn_dispatch() {
        let service: Arc<dyn AuthService> = Arc::new(StubAuth);
        let credentials = Credentials::Password {
            username: "alice".to_owned(),
            password: "wrong".to_owned(),
        };
        let error = block_on(service.authenticate(&credentials)).unwrap_err();
        // The structural shape survives dynamic dispatch: category, stable
        // code, and message.
        assert_eq!(error.category, ErrorCategory::Unauthenticated);
        assert_eq!(error.code(), 19);
        assert_eq!(error.message, "invalid credentials for \"alice\"");
        assert!(!error.category.is_retryable());
        assert_eq!(
            error.to_string(),
            "unauthenticated: invalid credentials for \"alice\""
        );
    }

    struct StubQuery;

    impl QueryService for StubQuery {
        fn prepare(
            &self,
            _session_id: SessionId,
            sql: String,
        ) -> BoxFuture<'_, PreparedStatementBinding> {
            Box::pin(async move {
                Err(CategoryError::new(
                    ErrorCategory::SchemaVersionMismatch,
                    format!("cannot prepare {sql:?}: stale catalog"),
                ))
            })
        }

        fn execute(&self, request: ExecuteRequest) -> BoxFuture<'_, ExecuteResponse> {
            Box::pin(async move {
                Ok(ExecuteResponse {
                    query_id: request.query_id,
                    rows_affected: 0,
                    frames: vec![b"frame-1".to_vec(), b"frame-2".to_vec()],
                })
            })
        }

        fn execute_stream(
            &self,
            _request: ExecuteRequest,
        ) -> BoxFuture<'_, Box<dyn ArrowFrameStream>> {
            Box::pin(async move {
                let stream: Box<dyn ArrowFrameStream> =
                    Box::new(vec![b"frame-1".to_vec(), b"frame-2".to_vec()].into_iter());
                Ok(stream)
            })
        }

        fn cancel_query(&self, query_id: QueryId) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                Err(CategoryError::new(
                    ErrorCategory::Cancelled,
                    format!("query {query_id} is not running"),
                ))
            })
        }

        fn get_query_status(&self, query_id: QueryId) -> BoxFuture<'_, QueryStatus> {
            Box::pin(async move {
                Ok(QueryStatus {
                    query_id,
                    phase: QueryPhase::Completed,
                    error: None,
                })
            })
        }
    }

    fn sample_request() -> ExecuteRequest {
        ExecuteRequest {
            request_id: [0x99; 16],
            query_id: QueryId::new_random(),
            session_id: Some(SessionId::from_bytes([0x88; 16])),
            database_id: DatabaseId::new_random(),
            principal: AuthenticatedIdentity::Credentialless,
            command: ExecuteCommand::Sql {
                text: "SELECT 1".to_owned(),
                params: vec![],
            },
            deadline_unix_micros: None,
            result_limits: ResultLimits::default(),
            resource_group: None,
            idempotency_key: None,
        }
    }

    #[test]
    fn query_service_methods_work_through_dyn_dispatch() {
        let service: Arc<dyn QueryService> = Arc::new(StubQuery);
        let request = sample_request();

        let response = block_on(service.execute(request.clone())).unwrap();
        assert_eq!(response.query_id, request.query_id);
        assert_eq!(response.frames.len(), 2);

        let mut stream = block_on(service.execute_stream(request)).unwrap();
        assert_eq!(
            block_on(stream.next_frame()).unwrap(),
            Some(b"frame-1".to_vec())
        );
        assert_eq!(
            block_on(stream.next_frame()).unwrap(),
            Some(b"frame-2".to_vec())
        );
        assert_eq!(block_on(stream.next_frame()).unwrap(), None);

        let status = block_on(service.get_query_status(response.query_id)).unwrap();
        assert_eq!(status.phase, QueryPhase::Completed);

        let error = block_on(service.cancel_query(response.query_id)).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Cancelled);

        let error = block_on(service.prepare(SessionId::ZERO, "SELECT 1".to_owned())).unwrap_err();
        assert_eq!(error.category, ErrorCategory::SchemaVersionMismatch);
    }

    #[test]
    fn vec_into_iter_stream_yields_all_frames_then_ends() {
        let mut stream: Box<dyn ArrowFrameStream> = Box::new(Vec::<Vec<u8>>::new().into_iter());
        assert_eq!(block_on(stream.next_frame()).unwrap(), None);
    }
}
