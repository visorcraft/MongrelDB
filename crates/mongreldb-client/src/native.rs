//! High-level native RPC client over pooled multiplexed TLS connections.

use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use mongreldb_protocol::native;
use mongreldb_protocol::native_transport::{NativeRpcClientConfig, NativeRpcClientPool};
use mongreldb_protocol::{NATIVE_API_MAJOR, NATIVE_API_MINOR};
use mongreldb_query::QueryId;
use prost::Message;

use crate::{ClientError, ClientResult, SecretString};

pub use mongreldb_protocol::native_transport::NativeRpcClientConfig as Config;

#[derive(Clone)]
pub struct NativeClient {
    pool: Arc<NativeRpcClientPool>,
    database_id: [u8; 16],
    request_timeout: Duration,
    max_retries: usize,
}

impl NativeClient {
    pub async fn connect(
        config: NativeRpcClientConfig,
        connections: usize,
        database_id: [u8; 16],
    ) -> ClientResult<Self> {
        let request_timeout = config.request_timeout;
        let pool = NativeRpcClientPool::connect(config, connections)
            .await
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        Ok(Self {
            pool,
            database_id,
            request_timeout,
            max_retries: 2,
        })
    }

    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub async fn authenticate_password(
        &self,
        username: impl Into<String>,
        password: &SecretString,
    ) -> ClientResult<NativeSession> {
        let username = username.into();
        let client_nonce = QueryId::random()
            .map_err(|error| ClientError::Transport(error.to_string()))?
            .to_string();
        let mut exchange = mongreldb_core::ScramClientSession::begin(
            &username,
            secrecy::ExposeSecret::expose_secret(password),
            &client_nonce,
        )
        .map_err(|error| ClientError::Transport(error.to_string()))?;
        let begin = self
            .pool
            .client()
            .auth()
            .begin_scram(native::BeginScramRequest {
                context: Some(context(self.request_timeout, None)?),
                username,
                client_first_bare: exchange.client_first_bare().into(),
                client_nonce,
            })
            .await
            .map_err(native_error)?
            .into_inner();
        let (client_final_without_proof, client_proof) = exchange
            .respond(&begin.server_first)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        let finish = self
            .pool
            .client()
            .auth()
            .finish_scram(native::FinishScramRequest {
                context: Some(context(self.request_timeout, None)?),
                exchange_id: begin.exchange_id,
                client_final_without_proof,
                client_proof,
            })
            .await
            .map_err(native_error)?
            .into_inner();
        exchange
            .verify_server_final(&finish.server_final)
            .map_err(|error| ClientError::Transport(error.to_string()))?;
        self.open_session(
            finish
                .authentication
                .ok_or_else(|| ClientError::Decode("SCRAM authentication missing".into()))?,
        )
        .await
    }

    pub async fn authenticate_anonymous(&self) -> ClientResult<NativeSession> {
        self.authenticate(None).await
    }

    /// Authenticate one MySQL wire handshake without exposing its password to
    /// the adapter. The native server validates the `caching_sha2_password`
    /// proof against the catalog verifier and returns a normal live session.
    pub async fn authenticate_mysql_caching_sha2(
        &self,
        username: impl Into<String>,
        nonce: &[u8],
        proof: &[u8],
    ) -> ClientResult<NativeSession> {
        self.authenticate(Some(
            native::authenticate_request::Credential::MysqlCachingSha2(
                native::MysqlCachingSha2Credential {
                    username: username.into(),
                    nonce: nonce.to_vec(),
                    proof: proof.to_vec(),
                },
            ),
        ))
        .await
    }

    async fn authenticate(
        &self,
        credential: Option<native::authenticate_request::Credential>,
    ) -> ClientResult<NativeSession> {
        let auth = self
            .pool
            .client()
            .auth()
            .authenticate(native::AuthenticateRequest {
                context: Some(context(self.request_timeout, None)?),
                credential,
            })
            .await
            .map_err(native_error)?
            .into_inner();
        self.open_session(auth).await
    }

    async fn open_session(
        &self,
        auth: native::AuthenticateResponse,
    ) -> ClientResult<NativeSession> {
        let session_id = self
            .pool
            .client()
            .session()
            .open_session(native::OpenSessionRequest {
                context: Some(context(self.request_timeout, None)?),
                identity: auth.identity,
                database_id: self.database_id.to_vec(),
                auth_token: auth.auth_token,
            })
            .await
            .map_err(native_error)?
            .into_inner()
            .session_id;
        Ok(NativeSession {
            pool: Arc::clone(&self.pool),
            session_id,
            database_id: self.database_id,
            request_timeout: self.request_timeout,
            max_retries: self.max_retries,
        })
    }
}

#[derive(Clone)]
pub struct NativeSession {
    pool: Arc<NativeRpcClientPool>,
    session_id: Vec<u8>,
    database_id: [u8; 16],
    request_timeout: Duration,
    max_retries: usize,
}

impl NativeSession {
    pub fn id(&self) -> &[u8] {
        &self.session_id
    }

    pub async fn close(self) -> ClientResult<()> {
        self.pool
            .client()
            .session()
            .close_session(native::CloseSessionRequest {
                context: Some(context(self.request_timeout, None)?),
                session_id: self.session_id,
            })
            .await
            .map_err(native_error)?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        table: impl Into<String>,
        schema: &mongreldb_core::Schema,
    ) -> ClientResult<native::CreateTableResponse> {
        if schema.columns.iter().any(|column| {
            matches!(
                column.embedding_source.as_ref(),
                Some(mongreldb_core::EmbeddingSource::LocalModel { .. })
            )
        }) {
            return Err(ClientError::Decode(
                "native CreateTable cannot transport node-local model paths; use ConfiguredModel or GeneratedColumnSpec"
                    .into(),
            ));
        }
        let full_schema_required = !schema.indexes.is_empty()
            || !schema.constraints.checks.is_empty()
            || schema
                .columns
                .iter()
                .any(|column| column.default_value.is_some() || column.embedding_source.is_some());
        let legacy_columns = schema
            .columns
            .iter()
            .map(native_column)
            .collect::<ClientResult<Vec<_>>>();
        let use_legacy_fields = !full_schema_required && legacy_columns.is_ok();
        let schema_json = serde_json::to_vec(schema).map_err(|error| {
            ClientError::Decode(format!("native schema encode failed: {error}"))
        })?;
        let response = self
            .pool
            .client()
            .catalog()
            .create_table(native::CreateTableRequest {
                context: Some(context(self.request_timeout, None)?),
                session_id: self.session_id.clone(),
                table: table.into(),
                schema_id: schema.schema_id,
                columns: if use_legacy_fields {
                    legacy_columns.unwrap_or_default()
                } else {
                    Vec::new()
                },
                uniques: if use_legacy_fields {
                    schema
                        .constraints
                        .uniques
                        .iter()
                        .map(|constraint| native::UniqueConstraint {
                            id: u32::from(constraint.id),
                            name: constraint.name.clone(),
                            columns: constraint.columns.iter().map(|id| u32::from(*id)).collect(),
                        })
                        .collect()
                } else {
                    Vec::new()
                },
                foreign_keys: if use_legacy_fields {
                    schema
                        .constraints
                        .foreign_keys
                        .iter()
                        .map(native_foreign_key)
                        .collect()
                } else {
                    Vec::new()
                },
                schema_json,
            })
            .await
            .map_err(native_error)?
            .into_inner();
        Ok(response)
    }

    pub async fn schema(&self, table: impl Into<String>) -> ClientResult<mongreldb_core::Schema> {
        let response = self
            .pool
            .client()
            .catalog()
            .get_schema(native::GetSchemaRequest {
                context: Some(context(self.request_timeout, None)?),
                database_id: self.database_id.to_vec(),
                table: table.into(),
                session_id: self.session_id.clone(),
            })
            .await
            .map_err(native_error)?
            .into_inner();
        if response.schema_json.is_empty() {
            return Err(ClientError::Decode(
                "native schema response omitted complete schema_json".into(),
            ));
        }
        serde_json::from_slice(&response.schema_json)
            .map_err(|error| ClientError::Decode(format!("native schema decode failed: {error}")))
    }

    pub async fn prepare(&self, sql: impl Into<String>) -> ClientResult<NativePrepared> {
        let response = self
            .pool
            .client()
            .query()
            .prepare(native::PrepareRequest {
                context: Some(context(self.request_timeout, None)?),
                session_id: self.session_id.clone(),
                sql: sql.into(),
            })
            .await
            .map_err(native_error)?
            .into_inner();
        Ok(NativePrepared {
            statement_id: response.statement_id,
            schema_version: response.schema_version,
        })
    }

    pub async fn execute(
        &self,
        sql: impl Into<String>,
        idempotency_key: Option<&str>,
    ) -> ClientResult<NativeExecuteResult> {
        let sql = sql.into();
        let retryable = idempotency_key.is_some() || read_only_sql(&sql);
        self.execute_request(
            native::execute_request::Command::Sql(sql),
            Vec::new(),
            idempotency_key,
            retryable,
        )
        .await
    }

    pub async fn execute_prepared(
        &self,
        statement: NativePrepared,
        parameters: &[mongreldb_protocol::request::ParameterValue],
    ) -> ClientResult<NativeExecuteResult> {
        let parameters = parameters
            .iter()
            .map(|parameter| {
                bincode::serialize(parameter)
                    .map_err(|error| ClientError::Decode(error.to_string()))
            })
            .collect::<ClientResult<Vec<_>>>()?;
        self.execute_request(
            native::execute_request::Command::PreparedStatementId(statement.statement_id),
            parameters,
            None,
            true,
        )
        .await
    }

    async fn execute_request(
        &self,
        command: native::execute_request::Command,
        parameters: Vec<Vec<u8>>,
        idempotency_key: Option<&str>,
        retryable: bool,
    ) -> ClientResult<NativeExecuteResult> {
        let mut attempt = 0;
        loop {
            let query_id = random_query_id()?;
            let request = native::ExecuteRequest {
                context: Some(context(self.request_timeout, idempotency_key)?),
                session_id: self.session_id.clone(),
                query_id: query_id.clone(),
                command: Some(command.clone()),
                parameters: parameters.clone(),
            };
            match self.pool.client().query().execute(request).await {
                Ok(response) => return decode_execute(response.into_inner()),
                Err(error)
                    if retryable && attempt < self.max_retries && native_retryable(&error) =>
                {
                    attempt += 1;
                }
                Err(error) => return Err(native_error(error)),
            }
        }
    }

    pub async fn execute_stream(&self, sql: impl Into<String>) -> ClientResult<NativeArrowStream> {
        let sql = sql.into();
        let mut attempt = 0;
        loop {
            let query_id = random_query_id()?;
            let request = native::ExecuteRequest {
                context: Some(context(self.request_timeout, None)?),
                session_id: self.session_id.clone(),
                query_id: query_id.clone(),
                command: Some(native::execute_request::Command::Sql(sql.clone())),
                parameters: Vec::new(),
            };
            match self.pool.client().query().execute_stream(request).await {
                Ok(response) => {
                    return Ok(NativeArrowStream {
                        query_id,
                        inner: response.into_inner(),
                        buffered: VecDeque::new(),
                        finished: false,
                    })
                }
                Err(error)
                    if read_only_sql(&sql)
                        && attempt < self.max_retries
                        && native_retryable(&error) =>
                {
                    attempt += 1;
                }
                Err(error) => return Err(native_error(error)),
            }
        }
    }

    pub async fn cancel(&self, query_id: &[u8]) -> ClientResult<()> {
        self.pool
            .client()
            .query()
            .cancel_query(native::CancelQueryRequest {
                context: Some(context(self.request_timeout, None)?),
                query_id: query_id.to_vec(),
                session_id: self.session_id.clone(),
            })
            .await
            .map_err(native_error)?;
        Ok(())
    }

    pub async fn query_status(&self, query_id: &[u8]) -> ClientResult<native::QueryStatusResponse> {
        self.pool
            .client()
            .query()
            .get_query_status(native::GetQueryStatusRequest {
                context: Some(context(self.request_timeout, None)?),
                query_id: query_id.to_vec(),
                session_id: self.session_id.clone(),
            })
            .await
            .map(tonic::Response::into_inner)
            .map_err(native_error)
    }

    pub async fn begin(&self, isolation: native::IsolationLevel) -> ClientResult<Vec<u8>> {
        self.pool
            .client()
            .transaction()
            .begin(native::BeginTransactionRequest {
                context: Some(context(self.request_timeout, None)?),
                session_id: self.session_id.clone(),
                isolation: isolation as i32,
            })
            .await
            .map(|response| response.into_inner().transaction_id)
            .map_err(native_error)
    }

    pub async fn commit(&self) -> ClientResult<()> {
        self.transaction("commit").await
    }

    pub async fn rollback(&self) -> ClientResult<()> {
        self.transaction("rollback").await
    }

    async fn transaction(&self, operation: &str) -> ClientResult<()> {
        let request = native::TransactionRequest {
            context: Some(context(self.request_timeout, None)?),
            session_id: self.session_id.clone(),
        };
        let mut client = self.pool.client().transaction();
        match operation {
            "commit" => client.commit(request).await,
            _ => client.rollback(request).await,
        }
        .map_err(native_error)?;
        Ok(())
    }
}

fn native_column(column: &mongreldb_core::ColumnDef) -> ClientResult<native::CreateColumn> {
    use mongreldb_core::TypeId;
    let (data_type, decimal_precision, decimal_scale) = match &column.ty {
        TypeId::Bool => (native::ColumnType::Bool, 0, 0),
        TypeId::Int8 => (native::ColumnType::Int8, 0, 0),
        TypeId::Int16 => (native::ColumnType::Int16, 0, 0),
        TypeId::Int32 => (native::ColumnType::Int32, 0, 0),
        TypeId::Int64 => (native::ColumnType::Int64, 0, 0),
        TypeId::UInt8 => (native::ColumnType::Uint8, 0, 0),
        TypeId::UInt16 => (native::ColumnType::Uint16, 0, 0),
        TypeId::UInt32 => (native::ColumnType::Uint32, 0, 0),
        TypeId::UInt64 => (native::ColumnType::Uint64, 0, 0),
        TypeId::Float32 => (native::ColumnType::Float32, 0, 0),
        TypeId::Float64 => (native::ColumnType::Float64, 0, 0),
        TypeId::TimestampNanos => (native::ColumnType::TimestampNanos, 0, 0),
        TypeId::Date32 => (native::ColumnType::Date32, 0, 0),
        TypeId::Date64 => (native::ColumnType::Date64, 0, 0),
        TypeId::Time64 => (native::ColumnType::Time64, 0, 0),
        TypeId::Bytes => (native::ColumnType::Bytes, 0, 0),
        TypeId::Json => (native::ColumnType::Json, 0, 0),
        TypeId::Decimal128 { precision, scale } => (
            native::ColumnType::Decimal128,
            u32::from(*precision),
            i32::from(*scale),
        ),
        _ => {
            return Err(ClientError::Decode(format!(
                "native CreateTable does not support column type {:?}",
                column.ty
            )))
        }
    };
    let allowed_flags = mongreldb_core::ColumnFlags::NULLABLE
        | mongreldb_core::ColumnFlags::PRIMARY_KEY
        | mongreldb_core::ColumnFlags::AUTO_INCREMENT;
    if column.flags.bits() & !allowed_flags != 0 {
        return Err(ClientError::Decode(format!(
            "native CreateTable does not support flags on column {:?}",
            column.name
        )));
    }
    Ok(native::CreateColumn {
        id: u32::from(column.id),
        name: column.name.clone(),
        data_type: data_type as i32,
        nullable: column.flags.contains(mongreldb_core::ColumnFlags::NULLABLE),
        primary_key: column
            .flags
            .contains(mongreldb_core::ColumnFlags::PRIMARY_KEY),
        decimal_precision,
        decimal_scale,
        auto_increment: column
            .flags
            .contains(mongreldb_core::ColumnFlags::AUTO_INCREMENT),
    })
}

fn native_foreign_key(constraint: &mongreldb_core::constraint::ForeignKey) -> native::ForeignKey {
    let action = |action| match action {
        mongreldb_core::constraint::FkAction::Restrict => native::ForeignKeyAction::Restrict,
        mongreldb_core::constraint::FkAction::Cascade => native::ForeignKeyAction::Cascade,
        mongreldb_core::constraint::FkAction::SetNull => native::ForeignKeyAction::SetNull,
    };
    native::ForeignKey {
        id: u32::from(constraint.id),
        name: constraint.name.clone(),
        columns: constraint.columns.iter().map(|id| u32::from(*id)).collect(),
        referenced_table: constraint.ref_table.clone(),
        referenced_columns: constraint
            .ref_columns
            .iter()
            .map(|id| u32::from(*id))
            .collect(),
        on_delete: action(constraint.on_delete) as i32,
        on_update: action(constraint.on_update) as i32,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativePrepared {
    pub statement_id: u64,
    pub schema_version: u64,
}

pub struct NativeExecuteResult {
    pub query_id: Vec<u8>,
    pub batches: Vec<RecordBatch>,
    pub rows_affected: u64,
    pub committed: bool,
    pub commit_epoch: Option<u64>,
    pub idempotency_replayed: bool,
    pub original_query_id: Vec<u8>,
}

pub struct NativeArrowStream {
    query_id: Vec<u8>,
    inner: tonic::Streaming<native::ArrowFrame>,
    buffered: VecDeque<RecordBatch>,
    finished: bool,
}

impl NativeArrowStream {
    pub fn query_id(&self) -> &[u8] {
        &self.query_id
    }

    pub async fn next_batch(&mut self) -> ClientResult<Option<RecordBatch>> {
        if let Some(batch) = self.buffered.pop_front() {
            return Ok(Some(batch));
        }
        while !self.finished {
            let Some(frame) = self.inner.message().await.map_err(native_error)? else {
                self.finished = true;
                return Ok(None);
            };
            if frame.end_of_stream {
                self.finished = true;
            }
            if frame.ipc.is_empty() {
                continue;
            }
            self.buffered.extend(decode_ipc(&frame.ipc)?);
            if let Some(batch) = self.buffered.pop_front() {
                return Ok(Some(batch));
            }
        }
        Ok(None)
    }
}

fn decode_execute(response: native::ExecuteResponse) -> ClientResult<NativeExecuteResult> {
    let mut batches = Vec::new();
    for frame in &response.frames {
        batches.extend(decode_ipc(&frame.ipc)?);
    }
    Ok(NativeExecuteResult {
        query_id: response.query_id,
        batches,
        rows_affected: response.rows_affected,
        committed: response.committed,
        commit_epoch: (response.commit_epoch != 0).then_some(response.commit_epoch),
        idempotency_replayed: response.idempotency_replayed,
        original_query_id: response.original_query_id,
    })
}

fn decode_ipc(ipc: &[u8]) -> ClientResult<Vec<RecordBatch>> {
    if ipc.is_empty() {
        return Ok(Vec::new());
    }
    StreamReader::try_new(Cursor::new(ipc), None)
        .map_err(|error| ClientError::Decode(error.to_string()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ClientError::Decode(error.to_string()))
}

fn context(
    timeout: Duration,
    idempotency_key: Option<&str>,
) -> ClientResult<native::RequestContext> {
    let request_id = QueryId::random()
        .map_err(|error| ClientError::Transport(error.to_string()))?
        .to_string();
    Ok(native::RequestContext {
        version: Some(native::ApiVersion {
            major: NATIVE_API_MAJOR,
            minor: NATIVE_API_MINOR,
        }),
        request_id,
        deadline_unix_micros: now_unix_micros()
            .saturating_add(timeout.as_micros().min(u128::from(u64::MAX)) as u64),
        idempotency_key: idempotency_key.unwrap_or_default().into(),
    })
}

fn random_query_id() -> ClientResult<Vec<u8>> {
    QueryId::random()
        .map(|id| id.as_bytes().to_vec())
        .map_err(|error| ClientError::Transport(error.to_string()))
}

fn now_unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

fn read_only_sql(sql: &str) -> bool {
    matches!(
        mongreldb_query::classify_sql_idempotency(sql),
        mongreldb_query::SqlIdempotencyClass::ReadOnly
    )
}

fn native_retryable(status: &tonic::Status) -> bool {
    if matches!(status.code(), tonic::Code::Unavailable) {
        return true;
    }
    native_detail(status).is_some_and(|detail| detail.retryable)
}

fn native_error(status: tonic::Status) -> ClientError {
    match native_detail(&status) {
        Some(detail) => ClientError::Native {
            code: format!("{:?}", status.code()),
            category_code: Some(detail.category_code),
            category: detail.category,
            message: detail.message,
            retryable: detail.retryable,
        },
        None => ClientError::Native {
            code: format!("{:?}", status.code()),
            category_code: None,
            category: "transport".into(),
            message: status.message().into(),
            retryable: status.code() == tonic::Code::Unavailable,
        },
    }
}

fn native_detail(status: &tonic::Status) -> Option<native::ErrorDetail> {
    (!status.details().is_empty())
        .then(|| native::ErrorDetail::decode(status.details()).ok())
        .flatten()
}
