//! TLS-only MySQL wire adapter over MongrelDB's canonical native RPC client.

use std::collections::HashMap;
use std::future::Future;
use std::io::{self, BufReader, Write};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use mongreldb_client::native::{
    NativeArrowStream, NativeClient, NativeExecuteResult, NativePrepared, NativeSession,
};
use mongreldb_protocol::native::IsolationLevel;
use mongreldb_protocol::request::ParameterValue;
use opensrv_mysql::{
    AsyncMysqlIntermediary, AsyncMysqlShim, Column, ColumnFlags, ColumnType, ErrorKind, InitWriter,
    IntermediaryOptions, OkResponse, ParamParser, QueryResultWriter, StatementMetaWriter,
    ToMysqlValue, ValueInner,
};
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::{version, ServerConfig};

const CACHING_SHA2_PASSWORD: &str = "caching_sha2_password";
const DEFAULT_MAX_PREPARED: usize = 1_024;

#[derive(Debug, thiserror::Error)]
pub enum MysqlWireError {
    #[error("MySQL wire TLS configuration failed: {0}")]
    Tls(#[from] io::Error),
    #[error("MySQL wire listener failed: {0}")]
    Listener(String),
}

#[derive(Debug, Clone)]
pub struct MysqlWireConfig {
    pub certificate_pem: Vec<u8>,
    pub private_key_pem: Vec<u8>,
    pub database_name: String,
    pub max_connections: usize,
    pub handshake_timeout: Duration,
}

/// Serve MySQL clients until `shutdown` resolves.
///
/// The adapter rejects non-TLS clients before authentication. Its
/// `caching_sha2_password` proof is verified by the native service against the
/// catalog verifier, so the wire adapter never receives a plaintext password.
pub async fn serve<F>(
    listener: TcpListener,
    config: MysqlWireConfig,
    client: NativeClient,
    shutdown: F,
) -> Result<(), MysqlWireError>
where
    F: Future<Output = ()>,
{
    let tls = Arc::new(tls13_config(
        &config.certificate_pem,
        &config.private_key_pem,
    )?);
    let permits = Arc::new(tokio::sync::Semaphore::new(config.max_connections.max(1)));
    let active = Arc::new(Mutex::new(HashMap::new()));
    let next_connection_id = Arc::new(AtomicU32::new(1));
    tokio::pin!(shutdown);
    loop {
        let accepted = tokio::select! {
            _ = &mut shutdown => return Ok(()),
            accepted = listener.accept() => accepted,
        };
        let (stream, _) = accepted.map_err(|error| MysqlWireError::Listener(error.to_string()))?;
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            continue;
        };
        let client = client.clone();
        let database_name = config.database_name.clone();
        let tls = Arc::clone(&tls);
        let active = Arc::clone(&active);
        let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
        let handshake_timeout = config.handshake_timeout;
        tokio::spawn(async move {
            let _permit = permit;
            let (mut reader, mut writer) = stream.into_split();
            let mut backend =
                Backend::new(client, database_name, connection_id, Arc::clone(&active));
            let initialized = tokio::time::timeout(
                handshake_timeout,
                AsyncMysqlIntermediary::init_before_ssl(
                    &mut backend,
                    &mut reader,
                    &mut writer,
                    &Some(Arc::clone(&tls)),
                ),
            )
            .await;
            let Ok(Ok((true, init))) = initialized else {
                return;
            };
            let options = IntermediaryOptions {
                process_use_statement_on_query: true,
                reject_connection_on_dbname_absence: false,
            };
            let _ =
                opensrv_mysql::secure_run_with_options(backend, writer, options, tls, init).await;
            if let Ok(mut active) = active.lock() {
                active.remove(&connection_id);
            }
        });
    }
}

fn tls13_config(certificate_pem: &[u8], private_key_pem: &[u8]) -> io::Result<ServerConfig> {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(certificate_pem))
        .collect::<Result<Vec<CertificateDer<'static>>, _>>()?;
    if certificates.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TLS certificate is missing",
        ));
    }
    let private_key = rustls_pemfile::private_key(&mut BufReader::new(private_key_pem))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS private key is missing"))?;
    ServerConfig::builder_with_protocol_versions(&[&version::TLS13])
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(io::Error::other)
}

#[derive(Clone)]
struct ActiveQuery {
    session: NativeSession,
    query_id: Vec<u8>,
}

struct Backend {
    client: NativeClient,
    database_name: String,
    connection_id: u32,
    session: tokio::sync::Mutex<Option<NativeSession>>,
    statements: HashMap<u32, NativePrepared>,
    next_statement_id: u32,
    active: Arc<Mutex<HashMap<u32, ActiveQuery>>>,
}

impl Backend {
    fn new(
        client: NativeClient,
        database_name: String,
        connection_id: u32,
        active: Arc<Mutex<HashMap<u32, ActiveQuery>>>,
    ) -> Self {
        Self {
            client,
            database_name,
            connection_id,
            session: tokio::sync::Mutex::new(None),
            statements: HashMap::new(),
            next_statement_id: 1,
            active,
        }
    }

    async fn authenticated_session(&self) -> io::Result<NativeSession> {
        self.session
            .lock()
            .await
            .clone()
            .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "not authenticated"))
    }

    async fn execute_query<W>(
        &self,
        query: &str,
        results: QueryResultWriter<'_, W>,
    ) -> io::Result<()>
    where
        W: AsyncWrite + Send + Unpin,
    {
        let session = self.authenticated_session().await?;
        let normalized = query.trim().trim_end_matches(';').trim();
        if normalized.eq_ignore_ascii_case("BEGIN")
            || normalized.eq_ignore_ascii_case("START TRANSACTION")
        {
            return complete(
                session.begin(IsolationLevel::RepeatableRead).await,
                results,
                0,
            )
            .await;
        }
        if normalized.eq_ignore_ascii_case("COMMIT") {
            return complete(session.commit().await, results, 0).await;
        }
        if normalized.eq_ignore_ascii_case("ROLLBACK") {
            return complete(session.rollback().await, results, 0).await;
        }
        if let Some(target) = kill_target(normalized) {
            let query = self
                .active
                .lock()
                .map_err(|_| io::Error::other("active query registry poisoned"))?
                .get(&target)
                .cloned();
            return match query {
                Some(query) => {
                    complete(query.session.cancel(&query.query_id).await, results, 0).await
                }
                None => {
                    results
                        .error(ErrorKind::ER_NO_SUCH_THREAD, b"unknown active connection")
                        .await
                }
            };
        }

        match session.execute_stream(query).await {
            Ok(stream) => {
                let query_id = stream.query_id().to_vec();
                self.active
                    .lock()
                    .map_err(|_| io::Error::other("active query registry poisoned"))?
                    .insert(self.connection_id, ActiveQuery { session, query_id });
                let written = write_stream(stream, results).await;
                if let Ok(mut active) = self.active.lock() {
                    active.remove(&self.connection_id);
                }
                written
            }
            Err(error) => mysql_error(results, &error.to_string()).await,
        }
    }
}

#[async_trait]
impl<W> AsyncMysqlShim<W> for Backend
where
    W: AsyncWrite + Send + Unpin,
{
    type Error = io::Error;

    fn version(&self) -> String {
        format!("8.0.0-mongreldb-{}", env!("CARGO_PKG_VERSION"))
    }

    fn connect_id(&self) -> u32 {
        self.connection_id
    }

    fn default_auth_plugin(&self) -> &str {
        CACHING_SHA2_PASSWORD
    }

    async fn auth_plugin_for_username(&self, _user: &[u8]) -> &str {
        CACHING_SHA2_PASSWORD
    }

    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        if auth_plugin != CACHING_SHA2_PASSWORD {
            return false;
        }
        let Ok(username) = std::str::from_utf8(username) else {
            return false;
        };
        match self
            .client
            .authenticate_mysql_caching_sha2(username, salt, auth_data)
            .await
        {
            Ok(session) => {
                *self.session.lock().await = Some(session);
                true
            }
            Err(_) => false,
        }
    }

    async fn on_prepare<'a>(
        &'a mut self,
        query: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        if self.statements.len() >= DEFAULT_MAX_PREPARED {
            return info
                .error(
                    ErrorKind::ER_MAX_PREPARED_STMT_COUNT_REACHED,
                    b"prepared statement limit reached",
                )
                .await;
        }
        let session = self.authenticated_session().await?;
        let (native_sql, parameter_count) = native_prepared_sql(query);
        let prepared = match session.prepare(native_sql).await {
            Ok(prepared) => prepared,
            Err(error) => {
                return info
                    .error(ErrorKind::ER_PARSE_ERROR, error.to_string().as_bytes())
                    .await
            }
        };
        let id = self.next_statement_id;
        self.next_statement_id = self.next_statement_id.wrapping_add(1).max(1);
        self.statements.insert(id, prepared);
        let parameters = (0..parameter_count)
            .map(|index| Column {
                table: String::new(),
                column: format!("param_{}", index + 1),
                coltype: ColumnType::MYSQL_TYPE_VAR_STRING,
                colflags: ColumnFlags::empty(),
            })
            .collect::<Vec<_>>();
        info.reply(id, &parameters, &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        id: u32,
        params: ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        let Some(statement) = self.statements.get(&id).copied() else {
            return results
                .error(ErrorKind::ER_UNKNOWN_STMT_HANDLER, b"unknown statement")
                .await;
        };
        let parameters = params
            .into_iter()
            .map(parameter_value)
            .collect::<io::Result<Vec<_>>>();
        let parameters = match parameters {
            Ok(parameters) => parameters,
            Err(error) => {
                return results
                    .error(ErrorKind::ER_WRONG_ARGUMENTS, error.to_string().as_bytes())
                    .await
            }
        };
        let session = self.authenticated_session().await?;
        match session.execute_prepared(statement, &parameters).await {
            Ok(result) => write_result(result, results).await,
            Err(error) => mysql_error(results, &error.to_string()).await,
        }
    }

    async fn on_close(&mut self, statement: u32) {
        self.statements.remove(&statement);
    }

    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        self.execute_query(query, results).await
    }

    async fn on_init<'a>(
        &'a mut self,
        database: &'a str,
        writer: InitWriter<'a, W>,
    ) -> io::Result<()> {
        if database == self.database_name {
            writer.ok().await
        } else {
            writer
                .error(ErrorKind::ER_BAD_DB_ERROR, b"unknown database")
                .await
        }
    }
}

fn kill_target(sql: &str) -> Option<u32> {
    let mut words = sql.split_ascii_whitespace();
    match (words.next(), words.next(), words.next(), words.next()) {
        (Some(kill), Some(query), Some(target), None)
            if kill.eq_ignore_ascii_case("KILL") && query.eq_ignore_ascii_case("QUERY") =>
        {
            target.parse().ok()
        }
        _ => None,
    }
}

fn native_prepared_sql(sql: &str) -> (String, usize) {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Quote(char),
        LineComment,
        BlockComment,
    }

    let mut output = String::with_capacity(sql.len());
    let mut count = 0;
    let mut state = State::Normal;
    let mut chars = sql.chars().peekable();
    while let Some(character) = chars.next() {
        output.push(character);
        match state {
            State::Normal if matches!(character, '\'' | '"' | '`') => {
                state = State::Quote(character);
            }
            State::Normal if character == '-' && chars.peek() == Some(&'-') => {
                output.push(chars.next().expect("peeked"));
                state = State::LineComment;
            }
            State::Normal if character == '#' => state = State::LineComment,
            State::Normal if character == '/' && chars.peek() == Some(&'*') => {
                output.push(chars.next().expect("peeked"));
                state = State::BlockComment;
            }
            State::Normal if character == '?' => {
                output.pop();
                count += 1;
                output.push('$');
                output.push_str(&count.to_string());
            }
            State::Quote(_) if character == '\\' => {
                if let Some(escaped) = chars.next() {
                    output.push(escaped);
                }
            }
            State::Quote(end) if character == end && chars.peek() == Some(&end) => {
                output.push(chars.next().expect("peeked"));
            }
            State::Quote(end) if character == end => state = State::Normal,
            State::LineComment if character == '\n' => state = State::Normal,
            State::BlockComment if character == '*' && chars.peek() == Some(&'/') => {
                output.push(chars.next().expect("peeked"));
                state = State::Normal;
            }
            _ => {}
        }
    }
    (output, count)
}

fn parameter_value(value: opensrv_mysql::ParamValue<'_>) -> io::Result<ParameterValue> {
    let textual = matches!(
        value.coltype,
        ColumnType::MYSQL_TYPE_STRING
            | ColumnType::MYSQL_TYPE_VAR_STRING
            | ColumnType::MYSQL_TYPE_VARCHAR
            | ColumnType::MYSQL_TYPE_ENUM
            | ColumnType::MYSQL_TYPE_SET
            | ColumnType::MYSQL_TYPE_JSON
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
    );
    match value.value.into_inner() {
        ValueInner::NULL => Ok(ParameterValue::Null),
        ValueInner::Int(value) => Ok(ParameterValue::Integer(value)),
        ValueInner::UInt(value) => {
            i64::try_from(value)
                .map(ParameterValue::Integer)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "unsigned parameter exceeds INT64",
                    )
                })
        }
        ValueInner::Double(value) if value.is_finite() => Ok(ParameterValue::Float(value)),
        ValueInner::Double(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "non-finite float parameter",
        )),
        ValueInner::Bytes(value) if textual => std::str::from_utf8(value)
            .map(|value| ParameterValue::Text(value.to_owned()))
            .map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "text parameter is not UTF-8")
            }),
        ValueInner::Bytes(value) => Ok(ParameterValue::Bytes(value.to_vec())),
        ValueInner::Date(value) => Ok(ParameterValue::Text(format_date(value)?)),
        ValueInner::Datetime(value) => Ok(ParameterValue::Text(format_datetime(value)?)),
        ValueInner::Time(value) => Ok(ParameterValue::Text(format_time(value)?)),
    }
}

fn format_date(value: &[u8]) -> io::Result<String> {
    if value.len() != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid MySQL date parameter",
        ));
    }
    Ok(format!(
        "{:04}-{:02}-{:02}",
        u16::from_le_bytes([value[0], value[1]]),
        value[2],
        value[3]
    ))
}

fn format_datetime(value: &[u8]) -> io::Result<String> {
    if !matches!(value.len(), 4 | 7 | 11) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid MySQL datetime parameter",
        ));
    }
    let mut output = format_date(&value[..4])?;
    if value.len() >= 7 {
        output.push_str(&format!(" {:02}:{:02}:{:02}", value[4], value[5], value[6]));
    }
    if value.len() == 11 {
        output.push_str(&format!(
            ".{:06}",
            u32::from_le_bytes([value[7], value[8], value[9], value[10]])
        ));
    }
    Ok(output)
}

fn format_time(value: &[u8]) -> io::Result<String> {
    if value.is_empty() {
        return Ok("00:00:00".into());
    }
    if !matches!(value.len(), 8 | 12) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid MySQL time parameter",
        ));
    }
    let days = u32::from_le_bytes([value[1], value[2], value[3], value[4]]);
    let mut output = format!(
        "{}{:02}:{:02}:{:02}",
        if value[0] == 0 { "" } else { "-" },
        days.saturating_mul(24) + u32::from(value[5]),
        value[6],
        value[7]
    );
    if value.len() == 12 {
        output.push_str(&format!(
            ".{:06}",
            u32::from_le_bytes([value[8], value[9], value[10], value[11]])
        ));
    }
    Ok(output)
}

async fn complete<T>(
    result: Result<T, mongreldb_client::ClientError>,
    writer: QueryResultWriter<'_, impl AsyncWrite + Unpin>,
    affected_rows: u64,
) -> io::Result<()> {
    match result {
        Ok(_) => {
            writer
                .completed(OkResponse {
                    affected_rows,
                    ..OkResponse::default()
                })
                .await
        }
        Err(error) => mysql_error(writer, &error.to_string()).await,
    }
}

async fn mysql_error<W>(writer: QueryResultWriter<'_, W>, message: &str) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer
        .error(ErrorKind::ER_UNKNOWN_ERROR, message.as_bytes())
        .await
}

async fn write_result<W>(
    result: NativeExecuteResult,
    writer: QueryResultWriter<'_, W>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    if result.batches.is_empty() {
        return writer
            .completed(OkResponse {
                affected_rows: result.rows_affected,
                ..OkResponse::default()
            })
            .await;
    }
    write_batches(&result.batches, writer).await
}

async fn write_stream<W>(
    mut stream: NativeArrowStream,
    writer: QueryResultWriter<'_, W>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let first = match stream.next_batch().await {
        Ok(Some(batch)) => batch,
        Ok(None) => return writer.completed(OkResponse::default()).await,
        Err(error) => return mysql_error(writer, &error.to_string()).await,
    };
    let columns = mysql_columns(&first);
    let mut rows = writer.start(&columns).await?;
    write_batch(&mut rows, &first).await?;
    loop {
        match stream.next_batch().await {
            Ok(Some(batch)) => write_batch(&mut rows, &batch).await?,
            Ok(None) => return rows.finish().await,
            Err(error) => return Err(io::Error::other(error)),
        }
    }
}

async fn write_batches<W>(
    batches: &[RecordBatch],
    writer: QueryResultWriter<'_, W>,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let columns = mysql_columns(&batches[0]);
    let mut rows = writer.start(&columns).await?;
    for batch in batches {
        write_batch(&mut rows, batch).await?;
    }
    rows.finish().await
}

fn mysql_columns(batch: &RecordBatch) -> Vec<Column> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let (coltype, unsigned) = match field.data_type() {
                DataType::Boolean => (ColumnType::MYSQL_TYPE_TINY, true),
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    (ColumnType::MYSQL_TYPE_LONGLONG, false)
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    (ColumnType::MYSQL_TYPE_LONGLONG, true)
                }
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    (ColumnType::MYSQL_TYPE_DOUBLE, false)
                }
                DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                    (ColumnType::MYSQL_TYPE_BLOB, false)
                }
                _ => (ColumnType::MYSQL_TYPE_VAR_STRING, false),
            };
            let mut flags = ColumnFlags::empty();
            flags.set(ColumnFlags::UNSIGNED_FLAG, unsigned);
            flags.set(ColumnFlags::NOT_NULL_FLAG, !field.is_nullable());
            Column {
                table: String::new(),
                column: field.name().clone(),
                coltype,
                colflags: flags,
            }
        })
        .collect()
}

async fn write_batch<W>(
    writer: &mut opensrv_mysql::RowWriter<'_, W>,
    batch: &RecordBatch,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    for row in 0..batch.num_rows() {
        for column in batch.columns() {
            writer.write_col(mysql_cell(column.as_ref(), row)?)?;
        }
        writer.end_row().await?;
    }
    Ok(())
}

enum MysqlCell {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl ToMysqlValue for MysqlCell {
    fn to_mysql_text<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Self::Null => Option::<u8>::None.to_mysql_text(writer),
            Self::Bool(value) => u8::from(*value).to_mysql_text(writer),
            Self::Signed(value) => value.to_mysql_text(writer),
            Self::Unsigned(value) => value.to_mysql_text(writer),
            Self::Float(value) => value.to_mysql_text(writer),
            Self::Text(value) => value.to_mysql_text(writer),
            Self::Bytes(value) => value.as_slice().to_mysql_text(writer),
        }
    }

    fn to_mysql_bin<W: Write>(&self, writer: &mut W, column: &Column) -> io::Result<()> {
        match self {
            Self::Null => unreachable!("NULL is encoded in the binary row null map"),
            Self::Bool(value) => u8::from(*value).to_mysql_bin(writer, column),
            Self::Signed(value) => value.to_mysql_bin(writer, column),
            Self::Unsigned(value) => value.to_mysql_bin(writer, column),
            Self::Float(value) => value.to_mysql_bin(writer, column),
            Self::Text(value) => value.to_mysql_bin(writer, column),
            Self::Bytes(value) => value.as_slice().to_mysql_bin(writer, column),
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }
}

fn mysql_cell(array: &dyn Array, row: usize) -> io::Result<MysqlCell> {
    if array.is_null(row) {
        return Ok(MysqlCell::Null);
    }
    macro_rules! value {
        ($array:ty, $variant:ident) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .map(|array| MysqlCell::$variant(array.value(row).into()))
        };
    }
    let cell = match array.data_type() {
        DataType::Boolean => value!(BooleanArray, Bool),
        DataType::Int8 => value!(Int8Array, Signed),
        DataType::Int16 => value!(Int16Array, Signed),
        DataType::Int32 => value!(Int32Array, Signed),
        DataType::Int64 => value!(Int64Array, Signed),
        DataType::UInt8 => value!(UInt8Array, Unsigned),
        DataType::UInt16 => value!(UInt16Array, Unsigned),
        DataType::UInt32 => value!(UInt32Array, Unsigned),
        DataType::UInt64 => value!(UInt64Array, Unsigned),
        DataType::Float32 => value!(Float32Array, Float),
        DataType::Float64 => value!(Float64Array, Float),
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| MysqlCell::Text(array.value(row).to_owned())),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|array| MysqlCell::Text(array.value(row).to_owned())),
        DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| MysqlCell::Bytes(array.value(row).to_vec())),
        DataType::LargeBinary => array
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .map(|array| MysqlCell::Bytes(array.value(row).to_vec())),
        _ => Some(MysqlCell::Text(
            arrow::util::display::array_value_to_string(array, row).map_err(io::Error::other)?,
        )),
    };
    cell.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Arrow array type mismatch"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameters_ignore_quoted_question_marks() {
        assert_eq!(
            native_prepared_sql("SELECT ?, '?', \"?\", `?`, ? -- ?\n/* ? */"),
            ("SELECT $1, '?', \"?\", `?`, $2 -- ?\n/* ? */".into(), 2)
        );
    }

    #[test]
    fn kill_query_parser_is_exact() {
        assert_eq!(kill_target("KILL QUERY 42"), Some(42));
        assert_eq!(kill_target("KILL 42"), None);
        assert_eq!(kill_target("KILL QUERY 42 trailing"), None);
    }
}
