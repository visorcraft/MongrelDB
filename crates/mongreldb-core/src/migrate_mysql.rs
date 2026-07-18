//! MySQL migration path (spec section 14.1, Stage 5A).
//!
//! Maps MySQL wire/dialect concepts into the canonical MongrelDB request
//! model. Does **not** duplicate the transaction engine: the adapter only
//! translates auth, COM_QUERY, prepared statements, transactions, metadata,
//! and kill into protocol requests.
//!
//! The migrate tool pipeline:
//! introspect → map types → recommend partition keys → generate DDL →
//! bounded copy → validate → binlog CDC catch-up → cutover → rollback window.
//! Application-level dual-write without outbox/CDC is explicitly not
//! recommended.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// MySQL→Mongrel type mapping entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeMapping {
    /// MySQL type name (e.g. `BIGINT UNSIGNED`).
    pub mysql_type: String,
    /// Mongrel type name (e.g. `UInt64`) or error marker.
    pub mongrel_type: String,
    /// Whether the mapping is lossy.
    pub lossy: bool,
    /// Notes for the operator.
    pub notes: String,
}

/// Dialect compatibility matrix row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DialectFeature {
    /// Feature key (e.g. `AUTO_INCREMENT`).
    pub feature: String,
    /// Support level.
    pub support: DialectSupport,
    /// Operator-facing detail.
    pub detail: String,
}

/// How fully a MySQL dialect feature is supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DialectSupport {
    /// Fully supported with equivalent semantics.
    Full,
    /// Supported with documented differences.
    Partial,
    /// Unsupported; clear error returned.
    Unsupported,
}

/// One table plan produced by introspection + mapping.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrateTablePlan {
    /// Source table name.
    pub source_table: String,
    /// Target table name.
    pub target_table: String,
    /// Column mappings (source → target type).
    pub columns: Vec<TypeMapping>,
    /// Recommended partition key columns (empty = single tablet).
    pub recommended_partition_keys: Vec<String>,
    /// Generated Mongrel DDL.
    pub ddl: String,
    /// Incompatible features flagged.
    pub incompatibilities: Vec<String>,
}

/// Full migration plan (schema + copy + CDC + cutover).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MysqlMigratePlan {
    /// Source DSN (redacted credentials for display).
    pub source_display: String,
    /// Target database id / path.
    pub target: String,
    /// Schema-only mode (no data copy).
    pub schema_only: bool,
    /// Per-table plans.
    pub tables: Vec<MigrateTablePlan>,
    /// Pipeline stages remaining.
    pub stages: Vec<MigrateStage>,
}

/// Pipeline stage of the migrate tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrateStage {
    /// Introspect MySQL schema.
    Introspect,
    /// Map types and constraints.
    MapTypes,
    /// Recommend partition/colocation keys.
    RecommendPartitioning,
    /// Generate MongrelDB DDL.
    GenerateDdl,
    /// Copy data in bounded batches.
    BoundedCopy,
    /// Validate row counts and checksums.
    Validate,
    /// Binlog CDC catch-up.
    CdcCatchUp,
    /// Cut over after lag reaches zero.
    Cutover,
    /// Rollback window open.
    RollbackWindow,
}

impl MigrateStage {
    /// Full pipeline order.
    pub const PIPELINE: [MigrateStage; 9] = [
        MigrateStage::Introspect,
        MigrateStage::MapTypes,
        MigrateStage::RecommendPartitioning,
        MigrateStage::GenerateDdl,
        MigrateStage::BoundedCopy,
        MigrateStage::Validate,
        MigrateStage::CdcCatchUp,
        MigrateStage::Cutover,
        MigrateStage::RollbackWindow,
    ];

    /// Stable name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Introspect => "introspect",
            Self::MapTypes => "map_types",
            Self::RecommendPartitioning => "recommend_partitioning",
            Self::GenerateDdl => "generate_ddl",
            Self::BoundedCopy => "bounded_copy",
            Self::Validate => "validate",
            Self::CdcCatchUp => "cdc_catch_up",
            Self::Cutover => "cutover",
            Self::RollbackWindow => "rollback_window",
        }
    }
}

/// Built-in dialect compatibility matrix (spec §14.1 list).
pub fn dialect_matrix() -> Vec<DialectFeature> {
    vec![
        feat(
            "data_types",
            DialectSupport::Partial,
            "common numeric/string/json types map; spatial unsupported",
        ),
        feat(
            "AUTO_INCREMENT",
            DialectSupport::Full,
            "maps to Mongrel sequences / auto-inc",
        ),
        feat(
            "LAST_INSERT_ID",
            DialectSupport::Partial,
            "session-scoped; multi-row insert returns first",
        ),
        feat(
            "ON DUPLICATE KEY UPDATE",
            DialectSupport::Partial,
            "maps to upsert when unique key present",
        ),
        feat(
            "LIMIT syntax",
            DialectSupport::Full,
            "LIMIT/OFFSET supported",
        ),
        feat(
            "boolean behavior",
            DialectSupport::Partial,
            "MySQL TINYINT(1) → Bool when declared",
        ),
        feat(
            "date/time behavior",
            DialectSupport::Partial,
            "TIMESTAMP TZ handling differs; documented",
        ),
        feat("JSON", DialectSupport::Full, "JSON type + path extracts"),
        feat(
            "collations",
            DialectSupport::Partial,
            "utf8mb4_bin equivalent; locale collations limited",
        ),
        feat(
            "isolation levels",
            DialectSupport::Full,
            "RC/RR/Serializable via SET TRANSACTION",
        ),
        feat(
            "locks",
            DialectSupport::Partial,
            "FOR UPDATE planned; GET_LOCK unsupported",
        ),
        feat(
            "information_schema",
            DialectSupport::Partial,
            "core tables exposed; full MySQL catalog not mirrored",
        ),
    ]
}

fn feat(feature: &str, support: DialectSupport, detail: &str) -> DialectFeature {
    DialectFeature {
        feature: feature.into(),
        support,
        detail: detail.into(),
    }
}

/// Map a MySQL column type name to a Mongrel type (best-effort matrix).
pub fn map_mysql_type(mysql_type: &str) -> TypeMapping {
    let upper = mysql_type.trim().to_ascii_uppercase();
    let (mongrel, lossy, notes) = match upper.as_str() {
        "TINYINT" | "TINYINT(1)" | "BOOL" | "BOOLEAN" => ("Bool", false, ""),
        "SMALLINT" => ("Int16", false, ""),
        "INT" | "INTEGER" | "MEDIUMINT" => ("Int32", false, ""),
        "BIGINT" => ("Int64", false, ""),
        "BIGINT UNSIGNED" => ("UInt64", false, ""),
        "FLOAT" => ("Float32", false, ""),
        "DOUBLE" | "DOUBLE PRECISION" | "REAL" => ("Float64", false, ""),
        t if t.starts_with("DECIMAL") || t.starts_with("NUMERIC") => {
            ("Decimal", true, "precision/scale preserved when declared")
        }
        t if t.starts_with("VARCHAR")
            || t.starts_with("CHAR")
            || t == "TEXT"
            || t == "TINYTEXT"
            || t == "MEDIUMTEXT"
            || t == "LONGTEXT" =>
        {
            ("Utf8", false, "")
        }
        t if t.starts_with("VARBINARY")
            || t.starts_with("BINARY")
            || t == "BLOB"
            || t == "TINYBLOB"
            || t == "MEDIUMBLOB"
            || t == "LONGBLOB" =>
        {
            ("Bytes", false, "")
        }
        "JSON" => ("Json", false, ""),
        "DATE" => ("Date", false, ""),
        "DATETIME" | "TIMESTAMP" => ("Timestamp", true, "timezone semantics differ"),
        "TIME" => ("Time", false, ""),
        other => ("Unsupported", true, other),
    };
    TypeMapping {
        mysql_type: mysql_type.into(),
        mongrel_type: mongrel.into(),
        lossy,
        notes: notes.into(),
    }
}

/// Build a migration plan from an introspected schema description.
///
/// `tables` maps table name → list of `(column_name, mysql_type)`.
pub fn plan_mysql_migration(
    source_display: impl Into<String>,
    target: impl Into<String>,
    schema_only: bool,
    tables: &BTreeMap<String, Vec<(String, String)>>,
) -> MysqlMigratePlan {
    let mut table_plans = Vec::new();
    for (name, cols) in tables {
        let columns: Vec<TypeMapping> = cols.iter().map(|(_, ty)| map_mysql_type(ty)).collect();
        let incompatibilities: Vec<String> = columns
            .iter()
            .filter(|c| c.mongrel_type == "Unsupported")
            .map(|c| format!("column type {} unsupported", c.mysql_type))
            .collect();
        let col_ddl: Vec<String> = cols
            .iter()
            .zip(columns.iter())
            .map(|((cname, _), mapping)| format!("  {} {}", cname, mapping.mongrel_type))
            .collect();
        let ddl = format!("CREATE TABLE {} (\n{}\n);", name, col_ddl.join(",\n"));
        // Recommend first integer PK-like column as partition key when present.
        let recommended_partition_keys = cols
            .iter()
            .find(|(_, ty)| {
                let u = ty.to_ascii_uppercase();
                u.contains("INT") && !u.contains("POINT")
            })
            .map(|(c, _)| vec![c.clone()])
            .unwrap_or_default();
        table_plans.push(MigrateTablePlan {
            source_table: name.clone(),
            target_table: name.clone(),
            columns,
            recommended_partition_keys,
            ddl,
            incompatibilities,
        });
    }
    let stages = if schema_only {
        MigrateStage::PIPELINE
            .into_iter()
            .filter(|s| {
                !matches!(
                    s,
                    MigrateStage::BoundedCopy
                        | MigrateStage::Validate
                        | MigrateStage::CdcCatchUp
                        | MigrateStage::Cutover
                        | MigrateStage::RollbackWindow
                )
            })
            .collect()
    } else {
        MigrateStage::PIPELINE.to_vec()
    };
    MysqlMigratePlan {
        source_display: source_display.into(),
        target: target.into(),
        schema_only,
        tables: table_plans,
        stages,
    }
}

/// Wire-adapter request kinds mapped into the canonical protocol model.
///
/// The adapter never implements its own transaction state machine.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MysqlWireRequest {
    /// COM_QUERY → ExecuteRequest SQL.
    Query {
        /// SQL text.
        sql: String,
    },
    /// COM_STMT_PREPARE.
    Prepare {
        /// SQL text.
        sql: String,
    },
    /// COM_STMT_EXECUTE.
    ExecutePrepared {
        /// Statement id.
        statement_id: u32,
        /// Bound parameter payloads (opaque).
        params: Vec<Vec<u8>>,
    },
    /// BEGIN/COMMIT/ROLLBACK → session txn commands.
    Transaction {
        /// begin | commit | rollback.
        verb: String,
    },
    /// COM_PROCESS_KILL / KILL QUERY.
    Kill {
        /// Target connection/query id.
        target_id: u64,
    },
    /// Auth handshake result (SCRAM or mysql_native_password mapped).
    Authenticate {
        /// Username.
        user: String,
    },
}

/// Dual-write warning constant (spec §14.1).
pub const DUAL_WRITE_WARNING: &str = "Do not recommend application-level dual writes without an \
outbox/CDC design. Use MySQL binlog CDC for migration catch-up.";

/// One source row as column name → string value (testable without a live MySQL).
pub type SourceRow = BTreeMap<String, String>;

/// Trait the migrate tool uses for source/target I/O. Production binds a
/// MySQL client + Mongrel session; tests supply in-memory stores.
pub trait MigrateIo {
    /// Apply generated DDL on the target.
    fn apply_ddl(&mut self, ddl: &str) -> Result<(), String>;
    /// Copy up to `batch` rows starting at `offset` from `table`.
    fn copy_batch(
        &mut self,
        table: &str,
        offset: u64,
        batch: u64,
    ) -> Result<Vec<SourceRow>, String>;
    /// Insert one row into the target table.
    fn insert_row(&mut self, table: &str, row: &SourceRow) -> Result<(), String>;
    /// Count rows on source and target for validation.
    fn count_rows(&self, table: &str) -> Result<(u64, u64), String>;
    /// Row checksum (deterministic) for source and target.
    fn checksum_rows(&self, table: &str) -> Result<(String, String), String>;
    /// Apply one CDC event (insert/update/delete as row map + op).
    fn apply_cdc(&mut self, table: &str, op: CdcOp, row: &SourceRow) -> Result<(), String>;
    /// Poll the source for more binlog events when lag remains after the
    /// initial catch-up batch. Implementations must block/back off rather than
    /// busy-spin and must honor `control`.
    fn poll_cdc(
        &mut self,
        _control: &crate::ExecutionControl,
    ) -> Result<Vec<(String, CdcOp, SourceRow)>, String> {
        Err("CDC source cannot poll remaining binlog events".into())
    }
    /// Current CDC lag in events remaining (0 = caught up).
    fn cdc_lag(&self) -> u64;
}

/// CDC operation kind from binlog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CdcOp {
    /// Insert.
    Insert,
    /// Update (row is post-image).
    Update,
    /// Delete.
    Delete,
}

/// Result of running the migrate pipeline against a [`MigrateIo`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrateRunReport {
    /// Stages completed in order.
    pub completed: Vec<MigrateStage>,
    /// Rows copied per table.
    pub rows_copied: BTreeMap<String, u64>,
    /// Validation ok.
    pub validated: bool,
    /// CDC lag at cutover.
    pub cdc_lag_at_cutover: u64,
    /// Whether cutover was performed.
    pub cut_over: bool,
    /// Rollback window still open.
    pub rollback_window_open: bool,
}

/// Default bounded copy batch size.
pub const DEFAULT_COPY_BATCH: u64 = 1_000;

/// Run the full migrate pipeline (or schema-only subset) against `io`.
///
/// Stages: map types already in `plan` → apply DDL → bounded copy → validate
/// counts/checksums → CDC catch-up until lag 0 → cutover → open rollback window.
/// Does **not** dual-write; CDC is the catch-up path (see [`DUAL_WRITE_WARNING`]).
pub fn run_migrate_pipeline(
    plan: &MysqlMigratePlan,
    io: &mut dyn MigrateIo,
    copy_batch: u64,
    cdc_events: &[(String, CdcOp, SourceRow)],
) -> Result<MigrateRunReport, String> {
    run_migrate_pipeline_controlled(
        plan,
        io,
        copy_batch,
        cdc_events,
        &crate::ExecutionControl::new(None),
    )
}

pub fn run_migrate_pipeline_controlled(
    plan: &MysqlMigratePlan,
    io: &mut dyn MigrateIo,
    copy_batch: u64,
    cdc_events: &[(String, CdcOp, SourceRow)],
    control: &crate::ExecutionControl,
) -> Result<MigrateRunReport, String> {
    let batch = copy_batch.max(1);
    let mut completed = Vec::new();
    let mut rows_copied: BTreeMap<String, u64> = BTreeMap::new();

    // Introspect/Map/Recommend already done when building the plan.
    completed.push(MigrateStage::Introspect);
    completed.push(MigrateStage::MapTypes);
    completed.push(MigrateStage::RecommendPartitioning);

    for table in &plan.tables {
        if !table.incompatibilities.is_empty() {
            return Err(format!(
                "table {} has incompatibilities: {:?}",
                table.source_table, table.incompatibilities
            ));
        }
        io.apply_ddl(&table.ddl)?;
    }
    completed.push(MigrateStage::GenerateDdl);

    if plan.schema_only {
        return Ok(MigrateRunReport {
            completed,
            rows_copied,
            validated: true,
            cdc_lag_at_cutover: 0,
            cut_over: false,
            rollback_window_open: false,
        });
    }

    for table in &plan.tables {
        let mut offset = 0u64;
        let mut total = 0u64;
        loop {
            let rows = io.copy_batch(&table.source_table, offset, batch)?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                io.insert_row(&table.target_table, row)?;
            }
            let n = rows.len() as u64;
            total += n;
            offset += n;
            if n < batch {
                break;
            }
        }
        rows_copied.insert(table.source_table.clone(), total);
    }
    completed.push(MigrateStage::BoundedCopy);

    for table in &plan.tables {
        let (src, dst) = io.count_rows(&table.source_table)?;
        if src != dst {
            return Err(format!(
                "row count mismatch on {}: source={src} target={dst}",
                table.source_table
            ));
        }
        let (cs, cd) = io.checksum_rows(&table.source_table)?;
        if cs != cd {
            return Err(format!(
                "checksum mismatch on {}: source={cs} target={cd}",
                table.source_table
            ));
        }
    }
    completed.push(MigrateStage::Validate);

    for (table, op, row) in cdc_events {
        control.checkpoint().map_err(|error| error.to_string())?;
        io.apply_cdc(table, *op, row)?;
    }
    // Drain remaining lag by polling real source progress. Positive lag with
    // no events or no decreasing watermark is a hard error, never a CPU spin.
    while io.cdc_lag() > 0 {
        control.checkpoint().map_err(|error| error.to_string())?;
        let before = io.cdc_lag();
        let events = io.poll_cdc(control)?;
        if events.is_empty() {
            return Err(format!(
                "CDC source made no progress while lag remained {before}"
            ));
        }
        for (table, op, row) in events {
            control.checkpoint().map_err(|error| error.to_string())?;
            io.apply_cdc(&table, op, &row)?;
        }
        let after = io.cdc_lag();
        if after >= before {
            return Err(format!(
                "CDC source watermark did not advance: before={before} after={after}"
            ));
        }
    }
    completed.push(MigrateStage::CdcCatchUp);

    let lag = io.cdc_lag();
    if lag != 0 {
        return Err(format!("refusing cutover with cdc lag {lag}"));
    }
    completed.push(MigrateStage::Cutover);
    completed.push(MigrateStage::RollbackWindow);

    Ok(MigrateRunReport {
        completed,
        rows_copied,
        validated: true,
        cdc_lag_at_cutover: lag,
        cut_over: true,
        rollback_window_open: true,
    })
}

/// In-memory migrate I/O for tests (source rows + target store).
#[derive(Debug, Default)]
pub struct MemoryMigrateIo {
    /// Source table → rows.
    pub source: BTreeMap<String, Vec<SourceRow>>,
    /// Target table → rows.
    pub target: BTreeMap<String, Vec<SourceRow>>,
    /// Applied DDL statements.
    pub ddl: Vec<String>,
    /// Pending CDC lag counter (decremented by apply_cdc or set by tests).
    pub lag: u64,
    /// Events returned by subsequent source polls.
    pub cdc_queue: Vec<(String, CdcOp, SourceRow)>,
}

impl MigrateIo for MemoryMigrateIo {
    fn apply_ddl(&mut self, ddl: &str) -> Result<(), String> {
        self.ddl.push(ddl.to_owned());
        Ok(())
    }

    fn copy_batch(
        &mut self,
        table: &str,
        offset: u64,
        batch: u64,
    ) -> Result<Vec<SourceRow>, String> {
        let rows = self.source.get(table).cloned().unwrap_or_default();
        let start = offset as usize;
        if start >= rows.len() {
            return Ok(Vec::new());
        }
        let end = (start + batch as usize).min(rows.len());
        Ok(rows[start..end].to_vec())
    }

    fn insert_row(&mut self, table: &str, row: &SourceRow) -> Result<(), String> {
        self.target
            .entry(table.to_owned())
            .or_default()
            .push(row.clone());
        Ok(())
    }

    fn count_rows(&self, table: &str) -> Result<(u64, u64), String> {
        let s = self.source.get(table).map(|r| r.len() as u64).unwrap_or(0);
        let t = self.target.get(table).map(|r| r.len() as u64).unwrap_or(0);
        Ok((s, t))
    }

    fn checksum_rows(&self, table: &str) -> Result<(String, String), String> {
        fn sum(rows: &[SourceRow]) -> String {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            for row in rows {
                for (k, v) in row {
                    h.update(k.as_bytes());
                    h.update([0]);
                    h.update(v.as_bytes());
                    h.update([0]);
                }
                h.update([1]);
            }
            format!("{:x}", h.finalize())
        }
        let s = self.source.get(table).map(|r| sum(r)).unwrap_or_default();
        let t = self.target.get(table).map(|r| sum(r)).unwrap_or_default();
        Ok((s, t))
    }

    fn apply_cdc(&mut self, table: &str, op: CdcOp, row: &SourceRow) -> Result<(), String> {
        match op {
            CdcOp::Insert | CdcOp::Update => {
                // Upsert by first column if present.
                let rows = self.target.entry(table.to_owned()).or_default();
                if let Some(key) = row.keys().next().cloned() {
                    if let Some(val) = row.get(&key) {
                        if let Some(existing) = rows.iter_mut().find(|r| r.get(&key) == Some(val)) {
                            *existing = row.clone();
                        } else {
                            rows.push(row.clone());
                        }
                    } else {
                        rows.push(row.clone());
                    }
                } else {
                    rows.push(row.clone());
                }
            }
            CdcOp::Delete => {
                if let Some((key, val)) = row.iter().next() {
                    if let Some(rows) = self.target.get_mut(table) {
                        rows.retain(|r| r.get(key) != Some(val));
                    }
                }
            }
        }
        self.lag = self.lag.saturating_sub(1);
        Ok(())
    }

    fn cdc_lag(&self) -> u64 {
        self.lag
    }

    fn poll_cdc(
        &mut self,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<(String, CdcOp, SourceRow)>, String> {
        control.checkpoint().map_err(|error| error.to_string())?;
        Ok(std::mem::take(&mut self.cdc_queue))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_common_types() {
        assert_eq!(map_mysql_type("BIGINT").mongrel_type, "Int64");
        assert_eq!(map_mysql_type("VARCHAR(255)").mongrel_type, "Utf8");
        assert_eq!(map_mysql_type("JSON").mongrel_type, "Json");
        assert!(map_mysql_type("GEOMETRY").mongrel_type == "Unsupported");
    }

    #[test]
    fn plan_pipeline_includes_cdc_not_dual_write() {
        let mut tables = BTreeMap::new();
        tables.insert(
            "orders".into(),
            vec![
                ("id".into(), "BIGINT".into()),
                ("note".into(), "TEXT".into()),
            ],
        );
        let plan = plan_mysql_migration("mysql://***@host/db", "mongrel://local", false, &tables);
        assert!(plan.stages.contains(&MigrateStage::CdcCatchUp));
        assert!(plan.stages.contains(&MigrateStage::BoundedCopy));
        assert_eq!(plan.tables.len(), 1);
        assert!(plan.tables[0].ddl.contains("CREATE TABLE orders"));
        assert_eq!(
            plan.tables[0].recommended_partition_keys,
            vec!["id".to_string()]
        );
        assert!(DUAL_WRITE_WARNING.contains("binlog CDC"));
    }

    #[test]
    fn run_pipeline_copy_validate_cdc_cutover() {
        let mut tables = BTreeMap::new();
        tables.insert(
            "orders".into(),
            vec![
                ("id".into(), "BIGINT".into()),
                ("note".into(), "TEXT".into()),
            ],
        );
        let plan = plan_mysql_migration("src", "dst", false, &tables);
        let mut io = MemoryMigrateIo::default();
        for i in 0..5u64 {
            let mut row = SourceRow::new();
            row.insert("id".into(), i.to_string());
            row.insert("note".into(), format!("n{i}"));
            io.source.entry("orders".into()).or_default().push(row);
        }
        // One insert that arrived after snapshot start.
        let mut cdc_row = SourceRow::new();
        cdc_row.insert("id".into(), "99".into());
        cdc_row.insert("note".into(), "late".into());
        io.lag = 1;
        let report = run_migrate_pipeline(
            &plan,
            &mut io,
            2, // small batches force multi-batch copy
            &[("orders".into(), CdcOp::Insert, cdc_row)],
        )
        .unwrap();
        assert!(report.cut_over);
        assert!(report.validated);
        assert!(report.rollback_window_open);
        assert_eq!(report.rows_copied.get("orders"), Some(&5));
        assert!(report.completed.contains(&MigrateStage::BoundedCopy));
        assert!(report.completed.contains(&MigrateStage::Validate));
        assert!(report.completed.contains(&MigrateStage::CdcCatchUp));
        assert!(report.completed.contains(&MigrateStage::Cutover));
        // Target has 5 copied + 1 CDC.
        assert_eq!(io.target.get("orders").map(|r| r.len()), Some(6));
        assert!(!io.ddl.is_empty());
    }

    #[test]
    fn dialect_matrix_covers_spec_list() {
        let m = dialect_matrix();
        for key in [
            "AUTO_INCREMENT",
            "ON DUPLICATE KEY UPDATE",
            "JSON",
            "isolation levels",
            "information_schema",
        ] {
            assert!(m.iter().any(|f| f.feature == key), "missing {key}");
        }
    }

    #[test]
    fn schema_only_skips_data_stages() {
        let plan = plan_mysql_migration("src", "dst", true, &BTreeMap::new());
        assert!(!plan.stages.iter().any(|s| matches!(
            s,
            MigrateStage::BoundedCopy | MigrateStage::CdcCatchUp | MigrateStage::Cutover
        )));
    }

    #[test]
    fn positive_cdc_lag_polls_events_instead_of_spinning() {
        let plan = MysqlMigratePlan {
            source_display: "src".into(),
            target: "dst".into(),
            schema_only: false,
            tables: Vec::new(),
            stages: MigrateStage::PIPELINE.to_vec(),
        };
        let mut row = SourceRow::new();
        row.insert("id".into(), "1".into());
        let mut io = MemoryMigrateIo {
            lag: 1,
            cdc_queue: vec![("orders".into(), CdcOp::Insert, row)],
            ..MemoryMigrateIo::default()
        };
        let report = run_migrate_pipeline(&plan, &mut io, 10, &[]).unwrap();
        assert!(report.cut_over);
        assert_eq!(io.cdc_lag(), 0);
    }

    #[test]
    fn positive_cdc_lag_without_events_fails_closed() {
        let plan = MysqlMigratePlan {
            source_display: "src".into(),
            target: "dst".into(),
            schema_only: false,
            tables: Vec::new(),
            stages: MigrateStage::PIPELINE.to_vec(),
        };
        let mut io = MemoryMigrateIo {
            lag: 1,
            ..MemoryMigrateIo::default()
        };
        assert_eq!(
            run_migrate_pipeline(&plan, &mut io, 10, &[]).unwrap_err(),
            "CDC source made no progress while lag remained 1"
        );
    }
}
