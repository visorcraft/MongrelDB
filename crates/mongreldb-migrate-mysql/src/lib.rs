//! Resumable MySQL snapshot and binlog migration over the native client.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::Array;
use futures::StreamExt;
use mongreldb_client::native::NativeSession;
use mongreldb_core::ExecutionControl;
use mongreldb_protocol::native::IsolationLevel;
use mysql_async::binlog::events::{EventData, RowsEventData};
use mysql_async::prelude::Queryable;
use mysql_async::{BinlogStreamRequest, Conn, Opts, Pool, Row, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("invalid MySQL source: {0}")]
    SourceConfig(String),
    #[error("MySQL source failed: {0}")]
    Source(#[from] mysql_async::Error),
    #[error("MongrelDB target failed: {0}")]
    Target(#[from] mongreldb_client::ClientError),
    #[error("migration checkpoint failed: {0}")]
    Checkpoint(#[from] std::io::Error),
    #[error("migration checkpoint is invalid: {0}")]
    InvalidCheckpoint(String),
    #[error("unsupported MySQL schema: {0}")]
    UnsupportedSchema(String),
    #[error("migration stopped: {0}")]
    Control(#[from] mongreldb_core::MongrelError),
}

pub type Result<T> = std::result::Result<T, MigrationError>;

#[derive(Clone)]
pub struct MysqlSource {
    pool: Pool,
    source_id: String,
    database: String,
}

impl MysqlSource {
    /// Opens a pool only when TLS, CA verification, and hostname verification
    /// are all enabled in the MySQL URL/options.
    pub fn new(url: &str) -> Result<Self> {
        let opts =
            Opts::from_url(url).map_err(|error| MigrationError::SourceConfig(error.to_string()))?;
        Self::from_opts(opts)
    }

    pub fn from_opts(opts: Opts) -> Result<Self> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let ssl = opts
            .ssl_opts()
            .ok_or_else(|| MigrationError::SourceConfig("require_ssl=true is required".into()))?;
        if ssl.accept_invalid_certs() || ssl.skip_domain_validation() {
            return Err(MigrationError::SourceConfig(
                "verify_ca=true and verify_identity=true are required".into(),
            ));
        }
        let database = opts
            .db_name()
            .filter(|name| !name.is_empty())
            .ok_or_else(|| MigrationError::SourceConfig("database name is required".into()))?
            .to_owned();
        let source_id = format!("{}:{}/{}", opts.ip_or_hostname(), opts.tcp_port(), database);
        Ok(Self {
            pool: Pool::new(opts),
            source_id,
            database,
        })
    }

    pub async fn preflight_cdc(&self, replica_server_id: u32) -> Result<()> {
        if replica_server_id == 0 {
            return Err(MigrationError::SourceConfig(
                "replica server_id must be non-zero".into(),
            ));
        }
        let mut connection = self.pool.get_conn().await?;
        let source_server_id: u32 = connection
            .query_first("SELECT @@GLOBAL.server_id")
            .await?
            .ok_or_else(|| MigrationError::SourceConfig("source server_id unavailable".into()))?;
        if source_server_id == 0 {
            return Err(MigrationError::SourceConfig(
                "source server_id must be non-zero".into(),
            ));
        }
        if source_server_id == replica_server_id {
            return Err(MigrationError::SourceConfig(
                "replica server_id must differ from source server_id".into(),
            ));
        }
        let grants = connection
            .query_map("SHOW GRANTS FOR CURRENT_USER()", |grant: String| grant)
            .await?;
        let has_replica = grants.iter().any(|grant| {
            let grant = grant.to_ascii_uppercase();
            grant.contains("ALL PRIVILEGES")
                || grant.contains("REPLICATION SLAVE")
                || grant.contains("REPLICATION REPLICA")
        });
        let has_client = grants.iter().any(|grant| {
            let grant = grant.to_ascii_uppercase();
            grant.contains("ALL PRIVILEGES") || grant.contains("REPLICATION CLIENT")
        });
        if !has_replica || !has_client {
            return Err(MigrationError::SourceConfig(
                "source user needs REPLICATION SLAVE/REPLICA and REPLICATION CLIENT privileges"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub async fn disconnect(self) -> Result<()> {
        self.pool.disconnect().await?;
        Ok(())
    }

    pub async fn introspect(&self) -> Result<SourceSchema> {
        let mut connection = self.pool.get_conn().await?;
        introspect_schema(&mut connection, &self.database).await
    }

    /// Holds a global read lock only while establishing the consistent
    /// snapshot and its exact binlog coordinate.
    pub async fn begin_consistent_snapshot(&self) -> Result<ConsistentSnapshot> {
        let mut lock = self.pool.get_conn().await?;
        lock.query_drop("FLUSH TABLES WITH READ LOCK").await?;
        let result = async {
            let mut connection = self.pool.get_conn().await?;
            connection
                .query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .await?;
            connection
                .query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
                .await?;
            let position = master_position(&mut lock).await?;
            Ok::<_, MigrationError>((connection, position))
        }
        .await;
        let unlock = lock.query_drop("UNLOCK TABLES").await;
        let (connection, position) = result?;
        unlock?;
        Ok(ConsistentSnapshot {
            connection,
            position,
        })
    }

    pub async fn catch_up(
        &self,
        schema: &SourceSchema,
        target: &NativeSession,
        checkpoint: &mut MigrationCheckpoint,
        store: &CheckpointStore,
        server_id: u32,
    ) -> Result<()> {
        let mut validation = self.pool.get_conn().await?;
        let (format, image): (String, String) = validation
            .query_first("SELECT @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image")
            .await?
            .ok_or_else(|| MigrationError::SourceConfig("binlog settings unavailable".into()))?;
        if format != "ROW" || image != "FULL" {
            return Err(MigrationError::SourceConfig(
                "binlog_format=ROW and binlog_row_image=FULL are required".into(),
            ));
        }
        let target_position = master_position(&mut validation).await?;
        drop(validation);

        checkpoint.stage = MigrationStage::CatchingUp;
        store.persist(checkpoint)?;
        let start = checkpoint.last_binlog_position.clone();
        let connection = self.pool.get_conn().await?;
        let mut stream = connection
            .get_binlog_stream(
                BinlogStreamRequest::new(server_id)
                    .with_filename(start.filename.as_bytes())
                    .with_pos(start.position)
                    .with_non_blocking(),
            )
            .await?;
        let tables = schema
            .tables
            .iter()
            .map(|table| (table.name.as_str(), table))
            .collect::<BTreeMap<_, _>>();
        let mut filename = start.filename;
        let mut current_gtid = None;
        let mut changes = Vec::new();
        while let Some(event) = stream.next().await {
            let event = event?;
            let event_position = u64::from(event.header().log_pos());
            match event
                .read_data()
                .map_err(|error| MigrationError::SourceConfig(error.to_string()))?
            {
                Some(EventData::RotateEvent(rotate)) => {
                    filename = rotate.name().into_owned();
                }
                Some(EventData::GtidEvent(gtid)) => {
                    current_gtid = Some(format!("{}:{}", format_sid(gtid.sid()), gtid.gno()));
                }
                Some(EventData::RowsEvent(rows)) => {
                    let table_map = stream.get_tme(rows.table_id()).ok_or_else(|| {
                        MigrationError::SourceConfig(format!(
                            "binlog table map {} is missing",
                            rows.table_id()
                        ))
                    })?;
                    if table_map.database_name().as_ref() != self.database {
                        continue;
                    }
                    let table_name = table_map.table_name().into_owned();
                    let table = tables.get(table_name.as_str()).ok_or_else(|| {
                        MigrationError::UnsupportedSchema(format!(
                            "binlog referenced unknown table {table_name}"
                        ))
                    })?;
                    let operation = row_operation(&rows);
                    for row in rows.rows(table_map) {
                        let (before, after) =
                            row.map_err(|error| MigrationError::SourceConfig(error.to_string()))?;
                        changes.push(BinlogChange {
                            table: (*table).clone(),
                            operation,
                            before: before.map(binlog_row_values).transpose()?,
                            after: after.map(binlog_row_values).transpose()?,
                        });
                    }
                }
                Some(EventData::XidEvent(_)) => {
                    let transaction_id = current_gtid
                        .take()
                        .unwrap_or_else(|| format!("{filename}:{event_position}"));
                    if !checkpoint.applied_transactions.contains(&transaction_id) {
                        apply_binlog_transaction(target, &changes).await?;
                    }
                    changes.clear();
                    checkpoint
                        .applied_transactions
                        .insert(transaction_id.clone());
                    while checkpoint.applied_transactions.len() > 4_096 {
                        checkpoint.applied_transactions.pop_first();
                    }
                    checkpoint.last_binlog_position = BinlogPosition {
                        filename: filename.clone(),
                        position: event_position,
                        executed_gtid_set: Some(transaction_id.clone()),
                    };
                    store.persist(checkpoint)?;
                }
                _ => {}
            }
        }
        stream.close().await?;
        if !changes.is_empty() {
            return Err(MigrationError::SourceConfig(
                "binlog stream ended inside a transaction".into(),
            ));
        }
        if position_before(&checkpoint.last_binlog_position, &target_position) {
            return Err(MigrationError::SourceConfig(format!(
                "CDC ended before target position {}:{}",
                target_position.filename, target_position.position
            )));
        }
        checkpoint.stage = MigrationStage::CutoverReady;
        store.persist(checkpoint)?;
        Ok(())
    }

    pub async fn catch_up_controlled(
        &self,
        schema: &SourceSchema,
        target: &NativeSession,
        checkpoint: &mut MigrationCheckpoint,
        store: &CheckpointStore,
        control: &ExecutionControl,
        options: &MigrationOptions,
    ) -> Result<()> {
        self.preflight_cdc(options.replica_server_id).await?;
        let mut attempt = 0;
        loop {
            control.checkpoint()?;
            match self
                .catch_up(schema, target, checkpoint, store, options.replica_server_id)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error)
                    if attempt < options.max_reconnect_attempts && retryable_cdc_error(&error) =>
                {
                    attempt += 1;
                    let delay = Duration::from_millis(100_u64.saturating_mul(1 << attempt.min(6)));
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = control.cancelled() => control.checkpoint()?,
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

fn retryable_cdc_error(error: &MigrationError) -> bool {
    matches!(error, MigrationError::Source(_))
        || matches!(
            error,
            MigrationError::SourceConfig(message)
                if message.starts_with("CDC ended before target position")
        )
}

#[derive(Debug, Clone, Copy)]
enum BinlogOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
struct BinlogChange {
    table: SourceTable,
    operation: BinlogOperation,
    before: Option<Vec<Value>>,
    after: Option<Vec<Value>>,
}

fn row_operation(rows: &RowsEventData<'_>) -> BinlogOperation {
    match rows {
        RowsEventData::WriteRowsEventV1(_) | RowsEventData::WriteRowsEvent(_) => {
            BinlogOperation::Insert
        }
        RowsEventData::UpdateRowsEventV1(_)
        | RowsEventData::UpdateRowsEvent(_)
        | RowsEventData::PartialUpdateRowsEvent(_) => BinlogOperation::Update,
        RowsEventData::DeleteRowsEventV1(_) | RowsEventData::DeleteRowsEvent(_) => {
            BinlogOperation::Delete
        }
    }
}

fn binlog_row_values(mut row: mysql_async::binlog::row::BinlogRow) -> Result<Vec<Value>> {
    (0..row.len())
        .map(|index| match row.take(index) {
            Some(value) => Value::try_from(value)
                .map_err(|error| MigrationError::SourceConfig(error.to_string())),
            None => Err(MigrationError::SourceConfig(
                "binlog row omitted a column; binlog_row_image must be FULL".into(),
            )),
        })
        .collect()
}

async fn apply_binlog_transaction(target: &NativeSession, changes: &[BinlogChange]) -> Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    target.begin(IsolationLevel::Serializable).await?;
    for change in changes {
        for sql in binlog_change_sql(change)? {
            if let Err(error) = target.execute(sql, None).await {
                let _ = target.rollback().await;
                return Err(error.into());
            }
        }
    }
    target.commit().await?;
    Ok(())
}

fn binlog_change_sql(change: &BinlogChange) -> Result<Vec<String>> {
    let after = || {
        change
            .after
            .as_ref()
            .ok_or_else(|| MigrationError::SourceConfig("binlog after-image is missing".into()))
    };
    Ok(match change.operation {
        BinlogOperation::Insert => vec![batch_insert_sql(
            &change.table,
            std::slice::from_ref(after()?),
        )?],
        BinlogOperation::Update => {
            let before = change.before.as_ref().ok_or_else(|| {
                MigrationError::SourceConfig("binlog before-image is missing".into())
            })?;
            let after = after()?;
            let mut statements = Vec::with_capacity(2);
            if primary_key_changed(&change.table, before, after)? {
                statements.push(delete_sql(&change.table, before)?);
            }
            statements.push(batch_insert_sql(
                &change.table,
                std::slice::from_ref(after),
            )?);
            statements
        }
        BinlogOperation::Delete => vec![delete_sql(
            &change.table,
            change.before.as_ref().ok_or_else(|| {
                MigrationError::SourceConfig("binlog before-image is missing".into())
            })?,
        )?],
    })
}

fn primary_key_changed(table: &SourceTable, before: &[Value], after: &[Value]) -> Result<bool> {
    for primary_key in &table.primary_key {
        let index = table
            .columns
            .iter()
            .position(|column| &column.name == primary_key)
            .ok_or_else(|| {
                MigrationError::UnsupportedSchema(format!(
                    "primary key {primary_key} missing from {}",
                    table.name
                ))
            })?;
        let before = before.get(index).ok_or_else(|| {
            MigrationError::SourceConfig(format!("short binlog before-image for {}", table.name))
        })?;
        let after = after.get(index).ok_or_else(|| {
            MigrationError::SourceConfig(format!("short binlog after-image for {}", table.name))
        })?;
        if before != after {
            return Ok(true);
        }
    }
    Ok(false)
}

fn delete_sql(table: &SourceTable, row: &[Value]) -> Result<String> {
    let mut predicates = Vec::new();
    for primary_key in &table.primary_key {
        let index = table
            .columns
            .iter()
            .position(|column| &column.name == primary_key)
            .ok_or_else(|| {
                MigrationError::UnsupportedSchema(format!(
                    "primary key {primary_key} missing from {}",
                    table.name
                ))
            })?;
        let value = row.get(index).ok_or_else(|| {
            MigrationError::SourceConfig(format!("short binlog row for {}", table.name))
        })?;
        predicates.push(if matches!(value, Value::NULL) {
            format!("{} IS NULL", quote_ident(primary_key))
        } else {
            format!(
                "{} = {}",
                quote_ident(primary_key),
                target_literal(&table.columns[index], value)?
            )
        });
    }
    Ok(format!(
        "DELETE FROM {} WHERE {}",
        quote_ident(&table.name),
        predicates.join(" AND ")
    ))
}

fn position_before(current: &BinlogPosition, target: &BinlogPosition) -> bool {
    current.filename < target.filename
        || (current.filename == target.filename && current.position < target.position)
}

fn format_sid(sid: [u8; 16]) -> String {
    let encoded = hex(&sid);
    format!(
        "{}-{}-{}-{}-{}",
        &encoded[0..8],
        &encoded[8..12],
        &encoded[12..16],
        &encoded[16..20],
        &encoded[20..32]
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSchema {
    pub database: String,
    pub tables: Vec<SourceTable>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTable {
    pub name: String,
    pub columns: Vec<SourceColumn>,
    pub primary_key: Vec<String>,
    pub unique_keys: Vec<SourceIndex>,
    pub secondary_indexes: Vec<SourceIndex>,
    pub foreign_keys: Vec<SourceForeignKey>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIndex {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceColumn {
    pub name: String,
    pub column_type: String,
    pub nullable: bool,
    pub auto_increment: bool,
    pub ordinal: u32,
    pub character_set: Option<String>,
    pub collation: Option<String>,
    pub generated_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
    #[serde(default)]
    pub on_delete: SourceFkAction,
    #[serde(default)]
    pub on_update: SourceFkAction,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SourceFkAction {
    #[default]
    Restrict,
    Cascade,
    SetNull,
}

pub fn target_ddl(table: &SourceTable) -> Result<String> {
    let mut definitions = table
        .columns
        .iter()
        .map(|column| {
            let mapping = mongreldb_core::map_mysql_type(&column.column_type);
            let ty = match mapping.mongrel_type.as_str() {
                "Bool" => "BOOLEAN".into(),
                "Int16" => "SMALLINT".into(),
                "Int32" => "INTEGER".into(),
                "Int64" => "BIGINT".into(),
                "UInt64" => "BIGINT UNSIGNED".into(),
                "Float32" => "REAL".into(),
                "Float64" => "DOUBLE".into(),
                "Decimal" => column.column_type.to_ascii_uppercase(),
                "Utf8" => "TEXT".into(),
                "Bytes" => "BLOB".into(),
                "Json" => "JSON".into(),
                "Date" => "DATE".into(),
                "Timestamp" => "TIMESTAMP".into(),
                "Time" => "TIME".into(),
                _ => {
                    return Err(MigrationError::UnsupportedSchema(format!(
                        "{}.{} has unsupported type {}",
                        table.name, column.name, column.column_type
                    )))
                }
            };
            Ok(format!(
                "{} {ty}{}",
                quote_ident(&column.name),
                if column.nullable { "" } else { " NOT NULL" }
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if !table.primary_key.is_empty() {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            table
                .primary_key
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for index in &table.unique_keys {
        definitions.push(format!(
            "CONSTRAINT {} UNIQUE ({})",
            quote_ident(&index.name),
            index
                .columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    for foreign_key in &table.foreign_keys {
        definitions.push(format!(
            "CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {} ({}) ON DELETE {} ON UPDATE {}",
            quote_ident(&foreign_key.name),
            foreign_key
                .columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            quote_ident(&foreign_key.referenced_table),
            foreign_key
                .referenced_columns
                .iter()
                .map(|column| quote_ident(column))
                .collect::<Vec<_>>()
                .join(", "),
            source_fk_action_sql(foreign_key.on_delete),
            source_fk_action_sql(foreign_key.on_update),
        ));
    }
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(&table.name),
        definitions.join(", ")
    ))
}

pub async fn apply_target_schema(
    schema: &SourceSchema,
    target: &NativeSession,
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
) -> Result<()> {
    let mut schema_version = checkpoint.target_schema_version.unwrap_or_default();
    let mut pending = schema.tables.iter().collect::<Vec<_>>();
    let mut created = BTreeSet::new();
    while !pending.is_empty() {
        let Some(index) = pending.iter().position(|table| {
            table.foreign_keys.iter().all(|foreign_key| {
                !schema
                    .tables
                    .iter()
                    .any(|table| table.name == foreign_key.referenced_table)
                    || created.contains(&foreign_key.referenced_table)
            })
        }) else {
            return Err(MigrationError::UnsupportedSchema(
                "cyclic foreign keys require manual two-phase DDL".into(),
            ));
        };
        let table = pending.remove(index);
        let result = target
            .create_table(&table.name, &target_schema(schema, table)?)
            .await?;
        schema_version = schema_version.max(result.schema_version);
        created.insert(table.name.clone());
    }
    checkpoint.target_schema_version = Some(schema_version);
    store.persist(checkpoint)?;
    Ok(())
}

fn target_schema(source: &SourceSchema, table: &SourceTable) -> Result<mongreldb_core::Schema> {
    let single_primary_key = (table.primary_key.len() == 1).then(|| table.primary_key[0].as_str());
    let columns = table
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let mut flags = mongreldb_core::ColumnFlags::empty();
            if column.nullable {
                flags = flags.with(mongreldb_core::ColumnFlags::NULLABLE);
            }
            if single_primary_key == Some(column.name.as_str()) {
                flags = flags.with(mongreldb_core::ColumnFlags::PRIMARY_KEY);
            }
            if column.auto_increment {
                if single_primary_key != Some(column.name.as_str()) {
                    return Err(MigrationError::UnsupportedSchema(format!(
                        "{}.{} AUTO_INCREMENT needs a single-column primary key",
                        table.name, column.name
                    )));
                }
                flags = flags.with(mongreldb_core::ColumnFlags::AUTO_INCREMENT);
            }
            Ok(mongreldb_core::ColumnDef {
                id: u16::try_from(index).map_err(|_| {
                    MigrationError::UnsupportedSchema(format!(
                        "{} has more than {} columns",
                        table.name,
                        u16::MAX
                    ))
                })?,
                name: column.name.clone(),
                ty: target_type(column)?,
                flags,
                default_value: None,
                embedding_source: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut unique_sources = table.unique_keys.clone();
    if table.primary_key.len() > 1 {
        unique_sources.insert(
            0,
            SourceIndex {
                name: "PRIMARY".into(),
                columns: table.primary_key.clone(),
            },
        );
    }
    let uniques = unique_sources
        .into_iter()
        .enumerate()
        .map(|(index, constraint)| {
            Ok(mongreldb_core::constraint::UniqueConstraint {
                id: u16::try_from(index + 1).map_err(|_| {
                    MigrationError::UnsupportedSchema(format!(
                        "{} has too many unique constraints",
                        table.name
                    ))
                })?,
                name: constraint.name,
                columns: constraint
                    .columns
                    .iter()
                    .map(|column| source_column_id(table, column))
                    .collect::<Result<Vec<_>>>()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let foreign_keys = table
        .foreign_keys
        .iter()
        .enumerate()
        .map(|(index, foreign_key)| {
            let referenced = source
                .tables
                .iter()
                .find(|candidate| candidate.name == foreign_key.referenced_table)
                .ok_or_else(|| {
                    MigrationError::UnsupportedSchema(format!(
                        "{} references missing table {}",
                        table.name, foreign_key.referenced_table
                    ))
                })?;
            Ok(mongreldb_core::constraint::ForeignKey {
                id: u16::try_from(index + 1).map_err(|_| {
                    MigrationError::UnsupportedSchema(format!(
                        "{} has too many foreign keys",
                        table.name
                    ))
                })?,
                name: foreign_key.name.clone(),
                columns: foreign_key
                    .columns
                    .iter()
                    .map(|column| source_column_id(table, column))
                    .collect::<Result<Vec<_>>>()?,
                ref_table: foreign_key.referenced_table.clone(),
                ref_columns: foreign_key
                    .referenced_columns
                    .iter()
                    .map(|column| source_column_id(referenced, column))
                    .collect::<Result<Vec<_>>>()?,
                on_delete: target_fk_action(foreign_key.on_delete),
                on_update: target_fk_action(foreign_key.on_update),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(mongreldb_core::Schema {
        schema_id: 0,
        columns,
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: mongreldb_core::constraint::TableConstraints {
            uniques,
            foreign_keys,
            checks: Vec::new(),
        },
        clustered: false,
    })
}

fn source_column_id(table: &SourceTable, name: &str) -> Result<u16> {
    let index = table
        .columns
        .iter()
        .position(|column| column.name == name)
        .ok_or_else(|| {
            MigrationError::UnsupportedSchema(format!("{}.{} does not exist", table.name, name))
        })?;
    u16::try_from(index).map_err(|_| {
        MigrationError::UnsupportedSchema(format!(
            "{}.{} exceeds column-id range",
            table.name, name
        ))
    })
}

fn source_fk_action(value: &str) -> Result<SourceFkAction> {
    match value {
        "RESTRICT" | "NO ACTION" => Ok(SourceFkAction::Restrict),
        "CASCADE" => Ok(SourceFkAction::Cascade),
        "SET NULL" => Ok(SourceFkAction::SetNull),
        _ => Err(MigrationError::UnsupportedSchema(format!(
            "unsupported foreign-key action {value}"
        ))),
    }
}

fn source_fk_action_sql(action: SourceFkAction) -> &'static str {
    match action {
        SourceFkAction::Restrict => "RESTRICT",
        SourceFkAction::Cascade => "CASCADE",
        SourceFkAction::SetNull => "SET NULL",
    }
}

fn target_fk_action(action: SourceFkAction) -> mongreldb_core::constraint::FkAction {
    match action {
        SourceFkAction::Restrict => mongreldb_core::constraint::FkAction::Restrict,
        SourceFkAction::Cascade => mongreldb_core::constraint::FkAction::Cascade,
        SourceFkAction::SetNull => mongreldb_core::constraint::FkAction::SetNull,
    }
}

fn target_type(column: &SourceColumn) -> Result<mongreldb_core::TypeId> {
    use mongreldb_core::TypeId;
    let mapping = mongreldb_core::map_mysql_type(&column.column_type);
    Ok(match mapping.mongrel_type.as_str() {
        "Bool" => TypeId::Bool,
        "Int8" => TypeId::Int8,
        "Int16" => TypeId::Int16,
        "Int32" => TypeId::Int32,
        "Int64" => TypeId::Int64,
        "UInt8" => TypeId::UInt8,
        "UInt16" => TypeId::UInt16,
        "UInt32" => TypeId::UInt32,
        "UInt64" => TypeId::UInt64,
        "Float32" => TypeId::Float32,
        "Float64" => TypeId::Float64,
        "Decimal" => {
            let (precision, scale) = mysql_decimal(&column.column_type)?;
            TypeId::Decimal128 { precision, scale }
        }
        "Utf8" | "Bytes" => TypeId::Bytes,
        "Json" => TypeId::Json,
        "Date" => TypeId::Date32,
        "Timestamp" => TypeId::TimestampNanos,
        "Time" => TypeId::Time64,
        _ => {
            return Err(MigrationError::UnsupportedSchema(format!(
                "{} has unsupported type {}",
                column.name, column.column_type
            )))
        }
    })
}

fn mysql_decimal(value: &str) -> Result<(u8, i8)> {
    let Some(parameters) = value
        .split_once('(')
        .and_then(|(_, tail)| tail.strip_suffix(')'))
    else {
        return Ok((10, 0));
    };
    let mut parameters = parameters.split(',').map(str::trim);
    let precision = parameters
        .next()
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or_else(|| MigrationError::UnsupportedSchema(format!("invalid decimal {value}")))?;
    let scale = parameters
        .next()
        .unwrap_or("0")
        .parse::<i8>()
        .map_err(|_| MigrationError::UnsupportedSchema(format!("invalid decimal {value}")))?;
    if parameters.next().is_some()
        || precision == 0
        || precision > 38
        || scale < 0
        || scale as u8 > precision
    {
        return Err(MigrationError::UnsupportedSchema(format!(
            "invalid decimal {value}"
        )));
    }
    Ok((precision, scale))
}

async fn introspect_schema(connection: &mut Conn, database: &str) -> Result<SourceSchema> {
    let columns: Vec<Row> = connection
        .exec(
            "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, ORDINAL_POSITION, \
             CHARACTER_SET_NAME, COLLATION_NAME, GENERATION_EXPRESSION, EXTRA \
             FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? \
             ORDER BY TABLE_NAME, ORDINAL_POSITION",
            (database,),
        )
        .await?;
    let mut tables = BTreeMap::<String, SourceTable>::new();
    for row in columns {
        let table: String = required(&row, "TABLE_NAME")?;
        let generated = row
            .get::<Option<String>, _>("GENERATION_EXPRESSION")
            .flatten()
            .unwrap_or_default();
        let extra: String = row.get("EXTRA").unwrap_or_default();
        let entry = tables.entry(table.clone()).or_insert_with(|| SourceTable {
            name: table,
            columns: Vec::new(),
            primary_key: Vec::new(),
            unique_keys: Vec::new(),
            secondary_indexes: Vec::new(),
            foreign_keys: Vec::new(),
            warnings: Vec::new(),
        });
        if !generated.is_empty() || extra.contains("GENERATED") {
            entry
                .warnings
                .push("generated column expression requires review".into());
        }
        entry.columns.push(SourceColumn {
            name: required(&row, "COLUMN_NAME")?,
            column_type: required(&row, "COLUMN_TYPE")?,
            nullable: row
                .get::<String, _>("IS_NULLABLE")
                .is_some_and(|value| value == "YES"),
            auto_increment: extra
                .split_ascii_whitespace()
                .any(|value| value.eq_ignore_ascii_case("auto_increment")),
            ordinal: required::<u32>(&row, "ORDINAL_POSITION")?,
            character_set: row.get::<Option<String>, _>("CHARACTER_SET_NAME").flatten(),
            collation: row.get::<Option<String>, _>("COLLATION_NAME").flatten(),
            generated_expression: (!generated.is_empty()).then_some(generated),
        });
    }

    let indexes: Vec<Row> = connection
        .exec(
            "SELECT TABLE_NAME, INDEX_NAME, NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX \
             FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? \
             ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
            (database,),
        )
        .await?;
    let mut keys = BTreeMap::<(String, String, bool), Vec<(u32, String)>>::new();
    for row in indexes {
        let table = required(&row, "TABLE_NAME")?;
        let name = required(&row, "INDEX_NAME")?;
        let non_unique = required::<u8>(&row, "NON_UNIQUE")? != 0;
        let sequence = required(&row, "SEQ_IN_INDEX")?;
        let column = required(&row, "COLUMN_NAME")?;
        keys.entry((table, name, non_unique))
            .or_default()
            .push((sequence, column));
    }
    for ((table, name, non_unique), mut columns) in keys {
        columns.sort_by_key(|(sequence, _)| *sequence);
        let columns = columns.into_iter().map(|(_, column)| column).collect();
        if let Some(table) = tables.get_mut(&table) {
            if name == "PRIMARY" {
                table.primary_key = columns;
            } else if !non_unique {
                table.unique_keys.push(SourceIndex { name, columns });
            } else {
                table.warnings.push(format!(
                    "non-unique index {name} requires target index-kind review"
                ));
                table.secondary_indexes.push(SourceIndex { name, columns });
            }
        }
    }

    let foreign_keys: Vec<Row> = connection
        .exec(
            "SELECT k.TABLE_NAME, k.CONSTRAINT_NAME, k.COLUMN_NAME, \
             k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, k.ORDINAL_POSITION, \
             r.DELETE_RULE, r.UPDATE_RULE \
             FROM information_schema.KEY_COLUMN_USAGE k \
             JOIN information_schema.REFERENTIAL_CONSTRAINTS r \
               ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA \
              AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME \
              AND r.TABLE_NAME = k.TABLE_NAME \
             WHERE k.TABLE_SCHEMA = ? AND k.REFERENCED_TABLE_NAME IS NOT NULL \
             ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME, k.ORDINAL_POSITION",
            (database,),
        )
        .await?;
    let mut grouped = BTreeMap::<
        (String, String, String, SourceFkAction, SourceFkAction),
        Vec<(u32, String, String)>,
    >::new();
    for row in foreign_keys {
        grouped
            .entry((
                required(&row, "TABLE_NAME")?,
                required(&row, "CONSTRAINT_NAME")?,
                required(&row, "REFERENCED_TABLE_NAME")?,
                source_fk_action(&required::<String>(&row, "DELETE_RULE")?)?,
                source_fk_action(&required::<String>(&row, "UPDATE_RULE")?)?,
            ))
            .or_default()
            .push((
                required(&row, "ORDINAL_POSITION")?,
                required(&row, "COLUMN_NAME")?,
                required(&row, "REFERENCED_COLUMN_NAME")?,
            ));
    }
    for ((table, name, referenced_table, on_delete, on_update), mut columns) in grouped {
        columns.sort_by_key(|(ordinal, _, _)| *ordinal);
        if let Some(table) = tables.get_mut(&table) {
            table.foreign_keys.push(SourceForeignKey {
                name,
                columns: columns
                    .iter()
                    .map(|(_, column, _)| column.clone())
                    .collect(),
                referenced_table,
                referenced_columns: columns.into_iter().map(|(_, _, column)| column).collect(),
                on_delete,
                on_update,
            });
        }
    }
    let triggers: Vec<String> = connection
        .exec_map(
            "SELECT EVENT_OBJECT_TABLE FROM information_schema.TRIGGERS \
             WHERE TRIGGER_SCHEMA = ? ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME",
            (database,),
            |table: String| table,
        )
        .await?;
    for table in triggers {
        if let Some(table) = tables.get_mut(&table) {
            table
                .warnings
                .push("source trigger requires manual migration review".into());
        }
    }
    Ok(SourceSchema {
        database: database.to_owned(),
        tables: tables.into_values().collect(),
    })
}

fn required<T: mysql_async::prelude::FromValue>(row: &Row, name: &str) -> Result<T> {
    row.get(name)
        .ok_or_else(|| MigrationError::SourceConfig(format!("information_schema omitted {name}")))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct BinlogPosition {
    pub filename: String,
    pub position: u64,
    pub executed_gtid_set: Option<String>,
}

async fn master_position(connection: &mut Conn) -> Result<BinlogPosition> {
    let row: Row = match connection.query_first("SHOW BINARY LOG STATUS").await {
        Ok(row) => row,
        Err(_) => connection.query_first("SHOW MASTER STATUS").await?,
    }
    .ok_or_else(|| MigrationError::SourceConfig("binary logging is disabled".into()))?;
    Ok(BinlogPosition {
        filename: required(&row, "File")?,
        position: required(&row, "Position")?,
        executed_gtid_set: row
            .get::<Option<String>, _>("Executed_Gtid_Set")
            .flatten()
            .filter(|value| !value.is_empty()),
    })
}

pub struct ConsistentSnapshot {
    connection: Conn,
    pub position: BinlogPosition,
}

impl ConsistentSnapshot {
    pub async fn read_batch(
        &mut self,
        table: &SourceTable,
        cursor: Option<&[CheckpointValue]>,
        limit: usize,
    ) -> Result<Vec<Vec<Value>>> {
        let order = if table.primary_key.is_empty() {
            return Err(MigrationError::UnsupportedSchema(format!(
                "table {} needs a primary key for resumable copy",
                table.name
            )));
        } else {
            table
                .primary_key
                .iter()
                .map(|column| quote_mysql_ident(column))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let predicate = match cursor {
            None => String::new(),
            Some(cursor) if cursor.len() == table.primary_key.len() => format!(
                " WHERE ({}) > ({})",
                table
                    .primary_key
                    .iter()
                    .map(|column| quote_mysql_ident(column))
                    .collect::<Vec<_>>()
                    .join(", "),
                cursor
                    .iter()
                    .map(CheckpointValue::to_mysql_literal)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Some(_) => {
                return Err(MigrationError::InvalidCheckpoint(format!(
                    "primary-key cursor width mismatch for {}",
                    table.name
                )))
            }
        };
        let sql = format!(
            "SELECT * FROM {}{predicate} ORDER BY {order} LIMIT {}",
            quote_mysql_ident(&table.name),
            limit.max(1)
        );
        let rows: Vec<Row> = self.connection.exec(sql, ()).await?;
        Ok(rows.into_iter().map(Row::unwrap).collect())
    }

    pub async fn commit(mut self) -> Result<()> {
        self.connection.query_drop("COMMIT").await?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStage {
    Planned,
    Copying,
    CatchingUp,
    CutoverReady,
    Cutover,
    RollbackWindow,
    Succeeded,
    Failed,
    RolledBack,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum CheckpointValue {
    Null,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(u32),
    Double(u64),
    Date(u16, u8, u8, u8, u8, u8, u32),
    Time(bool, u32, u8, u8, u8, u32),
}

impl CheckpointValue {
    fn from_mysql(value: &Value) -> Self {
        match value {
            Value::NULL => Self::Null,
            Value::Bytes(value) => Self::Bytes(value.clone()),
            Value::Int(value) => Self::Int(*value),
            Value::UInt(value) => Self::UInt(*value),
            Value::Float(value) => Self::Float(value.to_bits()),
            Value::Double(value) => Self::Double(value.to_bits()),
            Value::Date(year, month, day, hour, minute, second, micros) => {
                Self::Date(*year, *month, *day, *hour, *minute, *second, *micros)
            }
            Value::Time(negative, days, hours, minutes, seconds, micros) => {
                Self::Time(*negative, *days, *hours, *minutes, *seconds, *micros)
            }
        }
    }

    fn to_mysql_literal(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Bytes(value) => format!("X'{}'", hex(value)),
            Self::Int(value) => value.to_string(),
            Self::UInt(value) => value.to_string(),
            Self::Float(value) => f32::from_bits(*value).to_string(),
            Self::Double(value) => f64::from_bits(*value).to_string(),
            Self::Date(year, month, day, hour, minute, second, micros) => format!(
                "'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'"
            ),
            Self::Time(negative, days, hours, minutes, seconds, micros) => format!(
                "'{}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}'",
                if *negative { "-" } else { "" },
                hours = u32::from(*hours) + days * 24
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    pub source_id: String,
    pub schema_sha256: String,
    pub stage: MigrationStage,
    pub snapshot_position: BinlogPosition,
    pub table_offsets: BTreeMap<String, u64>,
    #[serde(default)]
    pub table_cursors: BTreeMap<String, Vec<CheckpointValue>>,
    #[serde(default)]
    pub source_row_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub target_row_counts: BTreeMap<String, u64>,
    #[serde(default)]
    pub source_checksums: BTreeMap<String, String>,
    #[serde(default)]
    pub target_checksums: BTreeMap<String, String>,
    pub last_binlog_position: BinlogPosition,
    pub applied_transactions: BTreeSet<String>,
    pub rollback_deadline_unix: Option<u64>,
    #[serde(default)]
    pub target_schema_version: Option<u64>,
    #[serde(default)]
    pub cutover_started_unix: Option<u64>,
    pub last_error: Option<String>,
}

impl MigrationCheckpoint {
    pub fn new(source: &MysqlSource, schema: &SourceSchema, position: BinlogPosition) -> Self {
        let encoded = serde_json::to_vec(schema).expect("SourceSchema serializes");
        Self {
            source_id: source.source_id.clone(),
            schema_sha256: format!("{:x}", Sha256::digest(encoded)),
            stage: MigrationStage::Planned,
            snapshot_position: position.clone(),
            table_offsets: BTreeMap::new(),
            table_cursors: BTreeMap::new(),
            source_row_counts: BTreeMap::new(),
            target_row_counts: BTreeMap::new(),
            source_checksums: BTreeMap::new(),
            target_checksums: BTreeMap::new(),
            last_binlog_position: position,
            applied_transactions: BTreeSet::new(),
            rollback_deadline_unix: None,
            target_schema_version: None,
            cutover_started_unix: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointStore {
    path: PathBuf,
}

impl CheckpointStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<MigrationCheckpoint>> {
        match std::fs::read(&self.path) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(|error| MigrationError::InvalidCheckpoint(error.to_string())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn persist(&self, checkpoint: &MigrationCheckpoint) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let temp = self.path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(checkpoint)
            .map_err(|error| MigrationError::InvalidCheckpoint(error.to_string()))?;
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&temp)?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
        }
        std::fs::rename(temp, &self.path)?;
        mongreldb_types::durability::sync_directory(parent)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct MigrationOptions {
    pub schema_only: bool,
    pub batch_size: usize,
    pub replica_server_id: u32,
    pub max_reconnect_attempts: usize,
    pub rollback_window: Duration,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            schema_only: false,
            batch_size: 1_000,
            replica_server_id: 4_294_000_001,
            max_reconnect_attempts: 8,
            rollback_window: Duration::from_secs(24 * 60 * 60),
        }
    }
}

pub async fn migrate<F>(
    source: &MysqlSource,
    target: &NativeSession,
    store: &CheckpointStore,
    options: &MigrationOptions,
    control: &ExecutionControl,
    publish_cutover: F,
) -> Result<MigrationCheckpoint>
where
    F: FnOnce() -> std::result::Result<(), String>,
{
    control.checkpoint()?;
    let schema = source.introspect().await?;
    let mut snapshot = source.begin_consistent_snapshot().await?;
    let mut checkpoint = match store.load()? {
        Some(checkpoint) => {
            validate_checkpoint(source, &schema, &checkpoint)?;
            checkpoint
        }
        None => {
            let checkpoint = MigrationCheckpoint::new(source, &schema, snapshot.position.clone());
            store.persist(&checkpoint)?;
            checkpoint
        }
    };
    apply_target_schema(&schema, target, &mut checkpoint, store).await?;
    if options.schema_only {
        snapshot.commit().await?;
        checkpoint.stage = MigrationStage::Succeeded;
        store.persist(&checkpoint)?;
        return Ok(checkpoint);
    }
    copy_snapshot_controlled(
        &mut snapshot,
        &schema,
        target,
        &mut checkpoint,
        store,
        options.batch_size,
        control,
    )
    .await?;
    snapshot.commit().await?;
    validate_target(&schema, target, &mut checkpoint, store, control).await?;
    cutover_with(
        source,
        target,
        &mut checkpoint,
        store,
        options,
        control,
        publish_cutover,
    )
    .await?;
    Ok(checkpoint)
}

pub async fn cutover_with<F>(
    source: &MysqlSource,
    target: &NativeSession,
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
    options: &MigrationOptions,
    control: &ExecutionControl,
    publish_cutover: F,
) -> Result<()>
where
    F: FnOnce() -> std::result::Result<(), String>,
{
    let schema = source.introspect().await?;
    validate_checkpoint(source, &schema, checkpoint)?;
    let mut lock = source.pool.get_conn().await?;
    lock.query_drop("FLUSH TABLES WITH READ LOCK").await?;
    let result = async {
        checkpoint.stage = MigrationStage::Cutover;
        checkpoint.cutover_started_unix = Some(now_unix_seconds());
        store.persist(checkpoint)?;
        source
            .catch_up_controlled(&schema, target, checkpoint, store, control, options)
            .await?;
        let current_schema = source.introspect().await?;
        if schema_sha256(&current_schema) != checkpoint.schema_sha256 {
            return Err(MigrationError::UnsupportedSchema(
                "source schema changed during migration".into(),
            ));
        }
        refresh_source_validation(source, &schema, checkpoint, store, control).await?;
        validate_target(&schema, target, checkpoint, store, control).await?;
        publish_cutover().map_err(MigrationError::SourceConfig)?;
        checkpoint.stage = MigrationStage::RollbackWindow;
        checkpoint.rollback_deadline_unix =
            Some(now_unix_seconds().saturating_add(options.rollback_window.as_secs()));
        store.persist(checkpoint)?;
        Ok(())
    }
    .await;
    let unlock = lock.query_drop("UNLOCK TABLES").await;
    result?;
    unlock?;
    Ok(())
}

pub fn rollback_with<F>(
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
    target_writes_since_cutover: u64,
    publish_rollback: F,
) -> Result<()>
where
    F: FnOnce() -> std::result::Result<(), String>,
{
    if checkpoint.stage != MigrationStage::RollbackWindow {
        return Err(MigrationError::InvalidCheckpoint(
            "rollback window is not active".into(),
        ));
    }
    if checkpoint
        .rollback_deadline_unix
        .is_none_or(|deadline| now_unix_seconds() > deadline)
    {
        return Err(MigrationError::InvalidCheckpoint(
            "rollback window expired".into(),
        ));
    }
    if target_writes_since_cutover != 0 {
        return Err(MigrationError::InvalidCheckpoint(format!(
            "rollback refused after {target_writes_since_cutover} target writes"
        )));
    }
    publish_rollback().map_err(MigrationError::SourceConfig)?;
    checkpoint.stage = MigrationStage::RolledBack;
    store.persist(checkpoint)?;
    Ok(())
}

fn validate_checkpoint(
    source: &MysqlSource,
    schema: &SourceSchema,
    checkpoint: &MigrationCheckpoint,
) -> Result<()> {
    if checkpoint.source_id != source.source_id {
        return Err(MigrationError::InvalidCheckpoint(
            "source identity changed".into(),
        ));
    }
    if checkpoint.schema_sha256 != schema_sha256(schema) {
        return Err(MigrationError::InvalidCheckpoint(
            "source schema changed".into(),
        ));
    }
    Ok(())
}

fn schema_sha256(schema: &SourceSchema) -> String {
    let encoded = serde_json::to_vec(schema).expect("SourceSchema serializes");
    format!("{:x}", Sha256::digest(encoded))
}

async fn refresh_source_validation(
    source: &MysqlSource,
    schema: &SourceSchema,
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
    control: &ExecutionControl,
) -> Result<()> {
    let connection = source.pool.get_conn().await?;
    let mut snapshot = ConsistentSnapshot {
        connection,
        position: checkpoint.last_binlog_position.clone(),
    };
    for table in &schema.tables {
        let mut cursor = None;
        let mut count = 0_u64;
        let mut checksum = String::new();
        loop {
            control.checkpoint()?;
            let rows = snapshot.read_batch(table, cursor.as_deref(), 1_000).await?;
            if rows.is_empty() {
                break;
            }
            checksum = source_checksum(&checksum, table, &rows)?;
            count += rows.len() as u64;
            let last = rows.last().expect("non-empty batch");
            cursor = Some(
                table
                    .primary_key
                    .iter()
                    .map(|primary_key| {
                        let index = table
                            .columns
                            .iter()
                            .position(|column| &column.name == primary_key)
                            .ok_or_else(|| {
                                MigrationError::UnsupportedSchema(format!(
                                    "primary key {primary_key} missing from {}",
                                    table.name
                                ))
                            })?;
                        last.get(index)
                            .map(CheckpointValue::from_mysql)
                            .ok_or_else(|| {
                                MigrationError::SourceConfig(format!(
                                    "short source row for {}",
                                    table.name
                                ))
                            })
                    })
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        checkpoint
            .source_row_counts
            .insert(table.name.clone(), count);
        checkpoint
            .source_checksums
            .insert(table.name.clone(), checksum);
        store.persist(checkpoint)?;
    }
    Ok(())
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub async fn copy_snapshot(
    snapshot: &mut ConsistentSnapshot,
    schema: &SourceSchema,
    target: &NativeSession,
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
    batch_size: usize,
) -> Result<()> {
    copy_snapshot_controlled(
        snapshot,
        schema,
        target,
        checkpoint,
        store,
        batch_size,
        &ExecutionControl::new(None),
    )
    .await
}

pub async fn copy_snapshot_controlled(
    snapshot: &mut ConsistentSnapshot,
    schema: &SourceSchema,
    target: &NativeSession,
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
    batch_size: usize,
    control: &ExecutionControl,
) -> Result<()> {
    checkpoint.stage = MigrationStage::Copying;
    store.persist(checkpoint)?;
    for table in &schema.tables {
        let mut offset = checkpoint
            .table_offsets
            .get(&table.name)
            .copied()
            .unwrap_or(0);
        loop {
            control.checkpoint()?;
            let cursor = checkpoint.table_cursors.get(&table.name).cloned();
            let rows = snapshot
                .read_batch(table, cursor.as_deref(), batch_size)
                .await?;
            if rows.is_empty() {
                break;
            }
            let sql = batch_insert_sql(table, &rows)?;
            target
                .execute(sql, Some(&format!("mysql-copy:{}:{offset}", table.name)))
                .await?;
            let last = rows.last().expect("non-empty batch");
            let next_cursor = table
                .primary_key
                .iter()
                .map(|primary_key| {
                    let index = table
                        .columns
                        .iter()
                        .position(|column| &column.name == primary_key)
                        .ok_or_else(|| {
                            MigrationError::UnsupportedSchema(format!(
                                "primary key {primary_key} missing from {}",
                                table.name
                            ))
                        })?;
                    last.get(index)
                        .map(CheckpointValue::from_mysql)
                        .ok_or_else(|| {
                            MigrationError::SourceConfig(format!(
                                "short snapshot row for {}",
                                table.name
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let checksum = source_checksum(
                checkpoint
                    .source_checksums
                    .get(&table.name)
                    .map(String::as_str)
                    .unwrap_or_default(),
                table,
                &rows,
            )?;
            offset += rows.len() as u64;
            checkpoint.table_offsets.insert(table.name.clone(), offset);
            checkpoint
                .source_row_counts
                .insert(table.name.clone(), offset);
            checkpoint
                .source_checksums
                .insert(table.name.clone(), checksum);
            checkpoint
                .table_cursors
                .insert(table.name.clone(), next_cursor);
            store.persist(checkpoint)?;
            if rows.len() < batch_size.max(1) {
                break;
            }
        }
    }
    Ok(())
}

fn source_checksum(previous: &str, table: &SourceTable, rows: &[Vec<Value>]) -> Result<String> {
    let mut checksum = previous.to_owned();
    for row in rows {
        if row.len() != table.columns.len() {
            return Err(MigrationError::SourceConfig(format!(
                "column count mismatch for {}",
                table.name
            )));
        }
        let mut hash = Sha256::new();
        hash.update(checksum.as_bytes());
        for (column, value) in table.columns.iter().zip(row) {
            let encoded = canonical_mysql_value(column, value);
            hash.update((encoded.len() as u64).to_le_bytes());
            hash.update(encoded);
        }
        hash.update(0_u64.to_le_bytes());
        checksum = format!("{:x}", hash.finalize());
    }
    Ok(checksum)
}

fn canonical_mysql_value(column: &SourceColumn, value: &Value) -> Vec<u8> {
    let bool_column = mongreldb_core::map_mysql_type(&column.column_type).mongrel_type == "Bool";
    match value {
        Value::NULL => vec![0],
        Value::Bytes(value) => {
            let mut output = vec![1];
            output.extend(value);
            output
        }
        Value::Int(value) if bool_column => {
            format!("2{}", if *value == 0 { "false" } else { "true" }).into_bytes()
        }
        Value::UInt(value) if bool_column => {
            format!("2{}", if *value == 0 { "false" } else { "true" }).into_bytes()
        }
        Value::Int(value) => format!("2{value}").into_bytes(),
        Value::UInt(value) => format!("2{value}").into_bytes(),
        Value::Float(value) => format!("2{value}").into_bytes(),
        Value::Double(value) => format!("2{value}").into_bytes(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format!("2{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{micros:06}")
                .into_bytes()
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => format!(
            "2{}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}",
            if *negative { "-" } else { "" },
            hours = u32::from(*hours) + days * 24
        )
        .into_bytes(),
    }
}

pub async fn validate_target(
    schema: &SourceSchema,
    target: &NativeSession,
    checkpoint: &mut MigrationCheckpoint,
    store: &CheckpointStore,
    control: &ExecutionControl,
) -> Result<()> {
    for table in &schema.tables {
        control.checkpoint()?;
        let order = table
            .primary_key
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", ");
        let result = target
            .execute(
                format!(
                    "SELECT * FROM {}{}",
                    quote_ident(&table.name),
                    if order.is_empty() {
                        String::new()
                    } else {
                        format!(" ORDER BY {order}")
                    }
                ),
                None,
            )
            .await?;
        let mut count = 0_u64;
        let mut checksum = String::new();
        for batch in &result.batches {
            for row in 0..batch.num_rows() {
                let mut hash = Sha256::new();
                hash.update(checksum.as_bytes());
                for array in batch.columns() {
                    let encoded = canonical_arrow_value(array.as_ref(), row)?;
                    hash.update((encoded.len() as u64).to_le_bytes());
                    hash.update(encoded);
                }
                hash.update(0_u64.to_le_bytes());
                checksum = format!("{:x}", hash.finalize());
                count += 1;
            }
        }
        checkpoint
            .target_row_counts
            .insert(table.name.clone(), count);
        checkpoint
            .target_checksums
            .insert(table.name.clone(), checksum.clone());
        store.persist(checkpoint)?;
        let source_count = checkpoint
            .source_row_counts
            .get(&table.name)
            .copied()
            .unwrap_or_default();
        if source_count != count {
            return Err(MigrationError::SourceConfig(format!(
                "row count mismatch on {}: source={source_count} target={count}",
                table.name
            )));
        }
        let source_checksum = checkpoint
            .source_checksums
            .get(&table.name)
            .map(String::as_str)
            .unwrap_or_default();
        if source_checksum != checksum {
            return Err(MigrationError::SourceConfig(format!(
                "checksum mismatch on {}: source={source_checksum} target={checksum}",
                table.name
            )));
        }
    }
    Ok(())
}

fn canonical_arrow_value(array: &dyn Array, row: usize) -> Result<Vec<u8>> {
    if array.is_null(row) {
        return Ok(vec![0]);
    }
    macro_rules! bytes {
        ($array:ty) => {
            array
                .as_any()
                .downcast_ref::<$array>()
                .map(|array| tagged_bytes(array.value(row)))
        };
    }
    use arrow::array::{BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
    let value = match array.data_type() {
        arrow::datatypes::DataType::Utf8 => bytes!(StringArray),
        arrow::datatypes::DataType::LargeUtf8 => bytes!(LargeStringArray),
        arrow::datatypes::DataType::Binary => bytes!(BinaryArray),
        arrow::datatypes::DataType::LargeBinary => bytes!(LargeBinaryArray),
        _ => {
            let value = arrow::util::display::array_value_to_string(array, row)
                .map_err(|error| MigrationError::SourceConfig(error.to_string()))?;
            Some(format!("2{value}").into_bytes())
        }
    };
    value.ok_or_else(|| MigrationError::SourceConfig("Arrow array type mismatch".into()))
}

fn tagged_bytes(value: impl AsRef<[u8]>) -> Vec<u8> {
    let value = value.as_ref();
    let mut output = Vec::with_capacity(value.len() + 1);
    output.push(1);
    output.extend_from_slice(value);
    output
}

fn batch_insert_sql(table: &SourceTable, rows: &[Vec<Value>]) -> Result<String> {
    let columns = table
        .columns
        .iter()
        .map(|column| quote_ident(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let values = rows
        .iter()
        .map(|row| {
            if row.len() != table.columns.len() {
                return Err(MigrationError::SourceConfig(format!(
                    "column count mismatch for {}",
                    table.name
                )));
            }
            Ok(format!(
                "({})",
                table
                    .columns
                    .iter()
                    .zip(row)
                    .map(|(column, value)| target_literal(column, value))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let updates = table
        .columns
        .iter()
        .filter(|column| !table.primary_key.contains(&column.name))
        .map(|column| {
            let name = quote_ident(&column.name);
            format!("{name} = excluded.{name}")
        })
        .collect::<Vec<_>>()
        .join(", ");
    let conflict = table
        .primary_key
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(if updates.is_empty() {
        format!(
            "INSERT INTO {} ({columns}) VALUES {values} ON CONFLICT ({conflict}) DO NOTHING",
            quote_ident(&table.name)
        )
    } else {
        format!(
            "INSERT INTO {} ({columns}) VALUES {values} ON CONFLICT ({conflict}) DO UPDATE SET {updates}",
            quote_ident(&table.name)
        )
    })
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_mysql_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn target_literal(column: &SourceColumn, value: &Value) -> Result<String> {
    Ok(match value {
        Value::NULL => "NULL".into(),
        Value::Bytes(bytes)
            if matches!(
                mongreldb_core::map_mysql_type(&column.column_type)
                    .mongrel_type
                    .as_str(),
                "Utf8" | "Json"
            ) =>
        {
            let value = std::str::from_utf8(bytes).map_err(|_| {
                MigrationError::SourceConfig(format!("{} contains invalid UTF-8", column.name))
            })?;
            format!("'{}'", value.replace('\'', "''"))
        }
        Value::Bytes(bytes) => format!("X'{}'", hex(bytes)),
        Value::Int(value) => value.to_string(),
        Value::UInt(value) => value.to_string(),
        Value::Float(value) => value.to_string(),
        Value::Double(value) => value.to_string(),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            format!("'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'")
        }
        Value::Time(negative, days, hours, minutes, seconds, micros) => format!(
            "'{}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}'",
            if *negative { "-" } else { "" },
            hours = u32::from(*hours) + days * 24
        ),
    })
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_requires_verified_tls() {
        assert!(matches!(
            MysqlSource::new("mysql://user:pass@localhost/db"),
            Err(MigrationError::SourceConfig(_))
        ));
        assert!(matches!(
            MysqlSource::new(
                "mysql://user:pass@localhost/db?require_ssl=true&verify_identity=false"
            ),
            Err(MigrationError::SourceConfig(_))
        ));
        MysqlSource::new(
            "mysql://user:pass@localhost/db?require_ssl=true&verify_ca=true&verify_identity=true",
        )
        .unwrap();
    }

    #[test]
    fn checkpoint_is_atomic_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let store = CheckpointStore::new(dir.path().join("checkpoint.json"));
        let checkpoint = MigrationCheckpoint {
            source_id: "source".into(),
            schema_sha256: "hash".into(),
            stage: MigrationStage::Copying,
            snapshot_position: BinlogPosition::default(),
            table_offsets: [("items".into(), 10)].into_iter().collect(),
            table_cursors: BTreeMap::new(),
            source_row_counts: BTreeMap::new(),
            target_row_counts: BTreeMap::new(),
            source_checksums: BTreeMap::new(),
            target_checksums: BTreeMap::new(),
            last_binlog_position: BinlogPosition::default(),
            applied_transactions: BTreeSet::new(),
            rollback_deadline_unix: None,
            target_schema_version: None,
            cutover_started_unix: None,
            last_error: None,
        };
        store.persist(&checkpoint).unwrap();
        assert_eq!(store.load().unwrap(), Some(checkpoint));
    }

    #[test]
    fn target_ddl_uses_target_identifier_quoting_for_unique_keys() {
        let ddl = target_ddl(&SourceTable {
            name: "users".into(),
            columns: vec![SourceColumn {
                name: "email".into(),
                column_type: "varchar(255)".into(),
                nullable: false,
                auto_increment: false,
                ordinal: 1,
                character_set: Some("utf8mb4".into()),
                collation: Some("utf8mb4_0900_ai_ci".into()),
                generated_expression: None,
            }],
            primary_key: Vec::new(),
            unique_keys: vec![SourceIndex {
                name: "email_unique".into(),
                columns: vec!["email".into()],
            }],
            secondary_indexes: Vec::new(),
            foreign_keys: Vec::new(),
            warnings: Vec::new(),
        })
        .unwrap();
        assert!(ddl.contains("UNIQUE (\"email\")"), "{ddl}");
        assert!(!ddl.contains('`'), "{ddl}");
    }

    #[test]
    fn foreign_key_actions_and_decimal_bounds_are_preserved() {
        assert_eq!(
            source_fk_action("NO ACTION").unwrap(),
            SourceFkAction::Restrict
        );
        assert_eq!(
            source_fk_action("CASCADE").unwrap(),
            SourceFkAction::Cascade
        );
        assert_eq!(
            source_fk_action("SET NULL").unwrap(),
            SourceFkAction::SetNull
        );
        assert!(source_fk_action("SET DEFAULT").is_err());
        assert_eq!(mysql_decimal("decimal(38,38)").unwrap(), (38, 38));
        assert!(mysql_decimal("decimal(2,3)").is_err());
    }

    #[test]
    fn primary_key_update_deletes_old_identity_before_upsert() {
        let table = SourceTable {
            name: "items".into(),
            columns: vec![
                SourceColumn {
                    name: "id".into(),
                    column_type: "bigint".into(),
                    nullable: false,
                    auto_increment: false,
                    ordinal: 1,
                    character_set: None,
                    collation: None,
                    generated_expression: None,
                },
                SourceColumn {
                    name: "name".into(),
                    column_type: "varchar(20)".into(),
                    nullable: false,
                    auto_increment: false,
                    ordinal: 2,
                    character_set: Some("utf8mb4".into()),
                    collation: Some("utf8mb4_0900_ai_ci".into()),
                    generated_expression: None,
                },
            ],
            primary_key: vec!["id".into()],
            unique_keys: Vec::new(),
            secondary_indexes: Vec::new(),
            foreign_keys: Vec::new(),
            warnings: Vec::new(),
        };
        let statements = binlog_change_sql(&BinlogChange {
            table,
            operation: BinlogOperation::Update,
            before: Some(vec![Value::Int(1), Value::Bytes(b"old".to_vec())]),
            after: Some(vec![Value::Int(2), Value::Bytes(b"new".to_vec())]),
        })
        .unwrap();
        assert_eq!(statements.len(), 2);
        assert!(statements[0].starts_with("DELETE FROM \"items\""));
        assert!(statements[1].starts_with("INSERT INTO \"items\""));
    }
}
