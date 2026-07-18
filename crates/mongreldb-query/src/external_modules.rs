use crate::extended_sql_functions::{json_table_batches_from_text, JsonTableMode};
use crate::{arrow_conv, MongrelQueryError, Result};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{
    BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use mongreldb_core::catalog::TableState;
use mongreldb_core::database::{TABLES_DIR, VTAB_DIR};
use mongreldb_core::schema::{
    ColumnDef as CoreColumnDef, ColumnFlags, Schema as CoreSchema, TypeId,
};
use mongreldb_core::{
    Database, ExecutionControl, ExternalTableEntry, ModuleArg, ModuleCapabilities, Value,
};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::query_registry::{RegisteredSqlQuery, SqlTaskContext};

const EXTERNAL_STATE_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const EXTERNAL_ROWS_LIMIT: usize = 1_000_000;
const EXTERNAL_ROWS_BYTES_LIMIT: usize = 64 * 1024 * 1024;
const EXTERNAL_BASE_WRITES_LIMIT: usize = 1_000_000;
const EXTERNAL_BASE_WRITE_BYTES_LIMIT: usize = 64 * 1024 * 1024;

fn external_resource_limit(
    resource: &'static str,
    requested: usize,
    limit: usize,
) -> MongrelQueryError {
    MongrelQueryError::Core(mongreldb_core::MongrelError::ResourceLimitExceeded {
        resource,
        requested,
        limit,
    })
}

fn external_df_checkpoint(control: &ExecutionControl) -> DFResult<()> {
    control
        .checkpoint()
        .map_err(|error| DataFusionError::Execution(error.to_string()))
}

fn controlled_module_result<T>(query: Option<&RegisteredSqlQuery>, result: Result<T>) -> Result<T> {
    match result {
        Ok(value) => {
            query.map(RegisteredSqlQuery::checkpoint).transpose()?;
            Ok(value)
        }
        Err(error) => match query.and_then(|query| query.checkpoint().err()) {
            Some(cancellation) => Err(cancellation),
            None => Err(error),
        },
    }
}

fn external_value_bytes(value: &Value) -> usize {
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

fn external_row_bytes(row: &HashMap<u16, Value>) -> usize {
    let bucket_bytes =
        std::mem::size_of::<(u16, Value)>().saturating_add(2 * std::mem::size_of::<usize>());
    row.capacity().saturating_mul(bucket_bytes).saturating_add(
        row.values()
            .map(external_value_bytes)
            .fold(0_usize, usize::saturating_add),
    )
}

pub(crate) fn enforce_external_rows_limit(
    rows: &[HashMap<u16, Value>],
    query: Option<&RegisteredSqlQuery>,
) -> Result<()> {
    if rows.len() > EXTERNAL_ROWS_LIMIT {
        return Err(external_resource_limit(
            "external table rows",
            rows.len(),
            EXTERNAL_ROWS_LIMIT,
        ));
    }
    let mut bytes = 0_usize;
    for (index, row) in rows.iter().enumerate() {
        if index % 256 == 0 {
            query.map(RegisteredSqlQuery::checkpoint).transpose()?;
        }
        bytes = bytes.saturating_add(external_row_bytes(row));
        if bytes > EXTERNAL_ROWS_BYTES_LIMIT {
            return Err(external_resource_limit(
                "external table row bytes",
                bytes,
                EXTERNAL_ROWS_BYTES_LIMIT,
            ));
        }
    }
    Ok(())
}

pub(crate) fn enforce_external_state_limit(state: &[u8]) -> Result<()> {
    if state.len() > EXTERNAL_STATE_BYTES_LIMIT {
        return Err(external_resource_limit(
            "external table state bytes",
            state.len(),
            EXTERNAL_STATE_BYTES_LIMIT,
        ));
    }
    Ok(())
}

pub trait ExternalTableModule: Send + Sync {
    fn name(&self) -> &str;
    fn descriptor(&self) -> ExternalModuleDescriptor;
    fn indexes_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        _entry: &ExternalTableEntry,
    ) -> Result<Vec<ExternalModuleIndex>> {
        context.control.checkpoint()?;
        Ok(Vec::new())
    }
    fn connect_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>>;
    fn read_rows_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Vec<HashMap<u16, Value>>> {
        context.control.checkpoint()?;
        Err(MongrelQueryError::Schema(format!(
            "external table {:?} using module {:?} is not row-writable",
            entry.name,
            self.name()
        )))
    }
    fn prepare_rows_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
        _rows: Vec<HashMap<u16, Value>>,
    ) -> Result<Vec<u8>> {
        context.control.checkpoint()?;
        Err(MongrelQueryError::Schema(format!(
            "external table {:?} using module {:?} is not row-writable",
            entry.name,
            self.name()
        )))
    }
    fn rows_from_state_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        state: &[u8],
    ) -> Result<Vec<HashMap<u16, Value>>> {
        context.control.checkpoint()?;
        if state.is_empty() {
            Ok(Vec::new())
        } else {
            decode_state_rows(state)
        }
    }
    fn write_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
        op: ExternalWriteOp,
        txn: &mut ExternalTxn,
    ) -> Result<ExternalWriteResult> {
        context.control.checkpoint()?;
        let (rows, changes) = match op {
            ExternalWriteOp::Insert { rows: inserted } => {
                let changes = inserted.len() as u64;
                let mut rows = txn.read_rows()?;
                rows.extend(inserted);
                (rows, changes)
            }
            ExternalWriteOp::ReplaceRows { rows, changes } => (rows, changes),
        };
        txn.replace_state(self.prepare_rows_with_control(context, entry, rows)?);
        context.control.checkpoint()?;
        Ok(ExternalWriteResult { changes })
    }
    fn destroy_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        _entry: &ExternalTableEntry,
    ) -> Result<()> {
        context.control.checkpoint()?;
        Ok(())
    }
}

pub trait ExternalTable: std::fmt::Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan>;
    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan>;
}

#[derive(Clone)]
pub struct ExternalModuleDescriptor {
    pub schema: CoreSchema,
    pub hidden_columns: Vec<String>,
    pub capabilities: ModuleCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalModuleIndex {
    pub name: String,
    pub column_ids: Vec<u16>,
    pub unique: bool,
    pub partial: bool,
}

impl ExternalModuleIndex {
    pub fn new(name: impl Into<String>, column_ids: Vec<u16>) -> Self {
        Self {
            name: name.into(),
            column_ids,
            unique: false,
            partial: false,
        }
    }
}

pub struct ExternalExecutionContext<'a> {
    pub database: &'a Arc<Database>,
    pub control: &'a ExecutionControl,
    pub query_id: Option<crate::QueryId>,
}

impl ExternalExecutionContext<'_> {
    pub fn raw_state(&self, entry: &ExternalTableEntry) -> Result<Vec<u8>> {
        self.control.checkpoint()?;
        let state = external_table_state_bytes(self.database, entry)?;
        self.control.checkpoint()?;
        Ok(state)
    }

    pub fn read_state(&self, entry: &ExternalTableEntry, key: &[u8]) -> Result<Option<Vec<u8>>> {
        ExternalTxn::read_state_from_bytes(&self.raw_state(entry)?, key)
    }
}

pub struct ExternalPlanRequest<'a> {
    pub projection: Option<Vec<usize>>,
    pub filters: Vec<ExternalFilter>,
    pub raw_filters: Vec<&'a Expr>,
    pub order_by: Vec<ExternalOrder>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub struct ExternalScan {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

#[derive(Clone)]
pub struct ExternalPlan {
    pub filter_pushdown: Vec<TableProviderFilterPushDown>,
    pub accepted_filters: Vec<AcceptedFilter>,
    pub residual_filters_required: bool,
    pub estimated_rows: Option<u64>,
    pub estimated_cost: f64,
    pub order_satisfied: bool,
    pub opaque: Arc<dyn Any + Send + Sync>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ExternalOrder {
    pub column_index: usize,
    pub descending: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExternalFilter {
    And(Vec<ExternalFilter>),
    Compare {
        column_index: usize,
        op: ExternalFilterOp,
        value: ScalarValue,
    },
    Unsupported {
        expr: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExternalFilterOp {
    Eq,
    NotEq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedFilter {
    pub filter_index: usize,
    pub pushdown: TableProviderFilterPushDown,
}

impl ExternalPlan {
    pub fn new(
        filter_pushdown: Vec<TableProviderFilterPushDown>,
        estimated_rows: Option<u64>,
        estimated_cost: f64,
        order_satisfied: bool,
    ) -> Self {
        let accepted_filters = filter_pushdown
            .iter()
            .enumerate()
            .filter_map(|(filter_index, pushdown)| match pushdown {
                TableProviderFilterPushDown::Exact | TableProviderFilterPushDown::Inexact => {
                    Some(AcceptedFilter {
                        filter_index,
                        pushdown: pushdown.clone(),
                    })
                }
                TableProviderFilterPushDown::Unsupported => None,
            })
            .collect();
        let residual_filters_required = filter_pushdown.iter().any(|pushdown| {
            matches!(
                pushdown,
                TableProviderFilterPushDown::Inexact | TableProviderFilterPushDown::Unsupported
            )
        });
        Self {
            filter_pushdown,
            accepted_filters,
            residual_filters_required,
            estimated_rows,
            estimated_cost,
            order_satisfied,
            opaque: Arc::new(()),
        }
    }

    pub fn with_opaque(mut self, opaque: Arc<dyn Any + Send + Sync>) -> Self {
        self.opaque = opaque;
        self
    }
}

fn external_filter_from_expr(expr: &Expr, schema: &ArrowSchema) -> ExternalFilter {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            ExternalFilter::And(vec![
                external_filter_from_expr(left, schema),
                external_filter_from_expr(right, schema),
            ])
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let Some(op) = external_filter_op(*op) else {
                return ExternalFilter::Unsupported {
                    expr: expr.to_string(),
                };
            };
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(value, _)) => schema
                    .index_of(&column.name)
                    .map(|column_index| ExternalFilter::Compare {
                        column_index,
                        op,
                        value: value.clone(),
                    })
                    .unwrap_or_else(|_| ExternalFilter::Unsupported {
                        expr: expr.to_string(),
                    }),
                (Expr::Literal(value, _), Expr::Column(column)) => schema
                    .index_of(&column.name)
                    .map(|column_index| ExternalFilter::Compare {
                        column_index,
                        op: op.flipped(),
                        value: value.clone(),
                    })
                    .unwrap_or_else(|_| ExternalFilter::Unsupported {
                        expr: expr.to_string(),
                    }),
                _ => ExternalFilter::Unsupported {
                    expr: expr.to_string(),
                },
            }
        }
        _ => ExternalFilter::Unsupported {
            expr: expr.to_string(),
        },
    }
}

fn external_filter_op(op: Operator) -> Option<ExternalFilterOp> {
    match op {
        Operator::Eq => Some(ExternalFilterOp::Eq),
        Operator::NotEq => Some(ExternalFilterOp::NotEq),
        Operator::Gt => Some(ExternalFilterOp::Gt),
        Operator::GtEq => Some(ExternalFilterOp::GtEq),
        Operator::Lt => Some(ExternalFilterOp::Lt),
        Operator::LtEq => Some(ExternalFilterOp::LtEq),
        _ => None,
    }
}

impl ExternalFilterOp {
    fn flipped(self) -> Self {
        match self {
            ExternalFilterOp::Eq => ExternalFilterOp::Eq,
            ExternalFilterOp::NotEq => ExternalFilterOp::NotEq,
            ExternalFilterOp::Gt => ExternalFilterOp::Lt,
            ExternalFilterOp::GtEq => ExternalFilterOp::LtEq,
            ExternalFilterOp::Lt => ExternalFilterOp::Gt,
            ExternalFilterOp::LtEq => ExternalFilterOp::GtEq,
        }
    }
}

#[derive(Clone)]
pub enum ExternalWriteOp {
    Insert {
        rows: Vec<HashMap<u16, Value>>,
    },
    ReplaceRows {
        rows: Vec<HashMap<u16, Value>>,
        changes: u64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExternalBaseWrite {
    Put {
        table: String,
        cells: Vec<(u16, Value)>,
    },
    Delete {
        table: String,
        row_id: u64,
    },
}

fn external_base_write_bytes(op: &ExternalBaseWrite) -> usize {
    match op {
        ExternalBaseWrite::Put { table, cells } => table.len().saturating_add(
            cells
                .iter()
                .map(|(_, value)| {
                    std::mem::size_of::<(u16, Value)>().saturating_add(external_value_bytes(value))
                })
                .fold(0_usize, usize::saturating_add),
        ),
        ExternalBaseWrite::Delete { table, .. } => {
            table.len().saturating_add(std::mem::size_of::<u64>())
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExternalWriteResult {
    pub changes: u64,
}

impl ExternalWriteResult {
    pub fn new(changes: u64) -> Self {
        Self { changes }
    }
}

pub struct ExternalTxn {
    state: Vec<u8>,
    base_writes: Vec<ExternalBaseWrite>,
    base_write_bytes: usize,
}

impl ExternalTxn {
    pub fn new(state: Vec<u8>) -> Self {
        Self {
            state,
            base_writes: Vec::new(),
            base_write_bytes: 0,
        }
    }

    pub fn read_state_from_bytes(state: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        enforce_external_state_limit(state)?;
        Ok(decode_kv_state(state)?.remove(key))
    }

    pub fn read_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Self::read_state_from_bytes(&self.state, key)
    }

    pub fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        enforce_external_state_limit(&self.state)?;
        if key.len().saturating_add(value.len()) > EXTERNAL_STATE_BYTES_LIMIT {
            return Err(external_resource_limit(
                "external transaction key/value bytes",
                key.len().saturating_add(value.len()),
                EXTERNAL_STATE_BYTES_LIMIT,
            ));
        }
        let mut state = decode_kv_state(&self.state)?;
        state.insert(key.to_vec(), value.to_vec());
        self.state = encode_kv_state(&state)?;
        Ok(())
    }

    pub fn delete_state(&mut self, key: &[u8]) -> Result<()> {
        enforce_external_state_limit(&self.state)?;
        let mut state = decode_kv_state(&self.state)?;
        state.remove(key);
        self.state = encode_kv_state(&state)?;
        Ok(())
    }

    pub fn replace_state(&mut self, state: Vec<u8>) {
        self.state = state;
    }

    pub fn read_rows(&self) -> Result<Vec<HashMap<u16, Value>>> {
        enforce_external_state_limit(&self.state)?;
        if self.state.is_empty() {
            Ok(Vec::new())
        } else {
            decode_state_rows(&self.state)
        }
    }

    pub fn replace_rows(
        &mut self,
        schema: &CoreSchema,
        rows: Vec<HashMap<u16, Value>>,
    ) -> Result<()> {
        enforce_external_rows_limit(&rows, None)?;
        validate_external_rows(schema, &rows)?;
        self.state = encode_state_rows(&rows)?;
        Ok(())
    }

    pub fn emit_base_write(&mut self, op: ExternalBaseWrite) -> Result<()> {
        if self.base_writes.len() >= EXTERNAL_BASE_WRITES_LIMIT {
            return Err(external_resource_limit(
                "external base write count",
                self.base_writes.len().saturating_add(1),
                EXTERNAL_BASE_WRITES_LIMIT,
            ));
        }
        let bytes = external_base_write_bytes(&op);
        let requested = self.base_write_bytes.saturating_add(bytes);
        if requested > EXTERNAL_BASE_WRITE_BYTES_LIMIT {
            return Err(external_resource_limit(
                "external base write bytes",
                requested,
                EXTERNAL_BASE_WRITE_BYTES_LIMIT,
            ));
        }
        self.base_write_bytes = requested;
        self.base_writes.push(op);
        Ok(())
    }

    fn into_parts(self) -> (Vec<u8>, Vec<ExternalBaseWrite>) {
        (self.state, self.base_writes)
    }
}

pub struct ExternalModuleRegistry {
    modules: parking_lot::RwLock<HashMap<String, Arc<dyn ExternalTableModule>>>,
}

impl Default for ExternalModuleRegistry {
    fn default() -> Self {
        let registry = Self {
            modules: parking_lot::RwLock::new(HashMap::new()),
        };
        for module in builtin_modules() {
            registry.register(module).expect("valid built-in module");
        }
        registry
    }
}

impl ExternalModuleRegistry {
    pub fn register(&self, module: Arc<dyn ExternalTableModule>) -> Result<()> {
        let name = normalize_module_name(module.name())?;
        self.modules.write().insert(name, module);
        Ok(())
    }

    pub fn names(&self) -> Vec<String> {
        let mut names = self.modules.read().keys().cloned().collect::<Vec<_>>();
        names.sort();
        names
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        normalize_module_name(name)
            .ok()
            .is_some_and(|name| self.modules.read().contains_key(&name))
    }

    pub(crate) fn descriptor(&self, name: &str) -> Result<ExternalModuleDescriptor> {
        Ok(self.module(name)?.descriptor())
    }

    pub(crate) fn external_table_provider(
        &self,
        db: &Arc<Database>,
        entry: &ExternalTableEntry,
        query: Option<&RegisteredSqlQuery>,
    ) -> Result<Arc<dyn TableProvider>> {
        let module = self.module(&entry.module)?;
        let fallback = ExecutionControl::new(None);
        let context = ExternalExecutionContext {
            database: db,
            control: query.map_or(&fallback, RegisteredSqlQuery::control),
            query_id: query.map(RegisteredSqlQuery::id),
        };
        query.map(RegisteredSqlQuery::checkpoint).transpose()?;
        let table = controlled_module_result(query, module.connect_with_control(&context, entry))?;
        Ok(Arc::new(ExternalTableProvider::new(
            Arc::clone(db),
            table,
            entry.capabilities,
        )))
    }

    pub(crate) fn external_table_indexes(
        &self,
        db: &Arc<Database>,
        entry: &ExternalTableEntry,
        query: &RegisteredSqlQuery,
    ) -> Result<Vec<ExternalModuleIndex>> {
        let module = self.module(&entry.module)?;
        let context = ExternalExecutionContext {
            database: db,
            control: query.control(),
            query_id: Some(query.id()),
        };
        query.checkpoint()?;
        let indexes =
            controlled_module_result(Some(query), module.indexes_with_control(&context, entry))?;
        Ok(indexes)
    }

    pub(crate) fn external_table_rows(
        &self,
        db: &Arc<Database>,
        entry: &ExternalTableEntry,
        query: &RegisteredSqlQuery,
    ) -> Result<Vec<HashMap<u16, Value>>> {
        let module = self.module(&entry.module)?;
        let context = ExternalExecutionContext {
            database: db,
            control: query.control(),
            query_id: Some(query.id()),
        };
        query.checkpoint()?;
        let rows =
            controlled_module_result(Some(query), module.read_rows_with_control(&context, entry))?;
        enforce_external_rows_limit(&rows, Some(query))?;
        query.checkpoint()?;
        Ok(rows)
    }

    pub(crate) fn external_table_rows_from_state(
        &self,
        db: &Arc<Database>,
        entry: &ExternalTableEntry,
        state: &[u8],
        query: &RegisteredSqlQuery,
    ) -> Result<Vec<HashMap<u16, Value>>> {
        enforce_external_state_limit(state)?;
        let module = self.module(&entry.module)?;
        let context = ExternalExecutionContext {
            database: db,
            control: query.control(),
            query_id: Some(query.id()),
        };
        query.checkpoint()?;
        let rows = controlled_module_result(
            Some(query),
            module.rows_from_state_with_control(&context, state),
        )?;
        enforce_external_rows_limit(&rows, Some(query))?;
        query.checkpoint()?;
        Ok(rows)
    }

    pub(crate) fn external_table_write(
        &self,
        db: &Arc<Database>,
        entry: &ExternalTableEntry,
        base_state: Vec<u8>,
        op: ExternalWriteOp,
        query: &RegisteredSqlQuery,
    ) -> Result<(Vec<u8>, ExternalWriteResult, Vec<ExternalBaseWrite>)> {
        enforce_external_state_limit(&base_state)?;
        match &op {
            ExternalWriteOp::Insert { rows } | ExternalWriteOp::ReplaceRows { rows, .. } => {
                enforce_external_rows_limit(rows, None)?;
            }
        }
        let module = self.module(&entry.module)?;
        let mut txn = ExternalTxn::new(base_state);
        let context = ExternalExecutionContext {
            database: db,
            control: query.control(),
            query_id: Some(query.id()),
        };
        query.checkpoint()?;
        let result = controlled_module_result(
            Some(query),
            module.write_with_control(&context, entry, op, &mut txn),
        )?;
        let (state, base_writes) = txn.into_parts();
        enforce_external_state_limit(&state)?;
        Ok((state, result, base_writes))
    }

    pub(crate) fn destroy_external_table(
        &self,
        db: &Arc<Database>,
        entry: &ExternalTableEntry,
        query: &RegisteredSqlQuery,
    ) -> Result<()> {
        let module = self.module(&entry.module)?;
        let context = ExternalExecutionContext {
            database: db,
            control: query.control(),
            query_id: Some(query.id()),
        };
        query.checkpoint()?;
        controlled_module_result(Some(query), module.destroy_with_control(&context, entry))?;
        Ok(())
    }

    fn module(&self, name: &str) -> Result<Arc<dyn ExternalTableModule>> {
        let name = normalize_module_name(name)?;
        self.modules.read().get(&name).cloned().ok_or_else(|| {
            MongrelQueryError::Schema(format!("external table module {name:?} is not registered"))
        })
    }
}

fn normalize_module_name(name: &str) -> Result<String> {
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return Err(MongrelQueryError::Schema(format!(
            "invalid external table module name {name:?}"
        )));
    }
    Ok(name)
}

fn builtin_modules() -> Vec<Arc<dyn ExternalTableModule>> {
    vec![
        Arc::new(JsonModule {
            name: "json_each",
            mode: JsonTableMode::Each,
        }),
        Arc::new(JsonModule {
            name: "json_tree",
            mode: JsonTableMode::Tree,
        }),
        Arc::new(JsonModule {
            name: "jsonb_each",
            mode: JsonTableMode::Each,
        }),
        Arc::new(JsonModule {
            name: "jsonb_tree",
            mode: JsonTableMode::Tree,
        }),
        Arc::new(SchemaTablesModule),
        Arc::new(DbStatModule),
        Arc::new(FtsDocsModule),
        Arc::new(KvStoreModule),
        Arc::new(RTreeRectsModule),
        Arc::new(SeriesModule),
    ]
}

struct ExternalTableProvider {
    db: Arc<Database>,
    table: Arc<dyn ExternalTable>,
    cache_plans: bool,
    plan_cache: parking_lot::Mutex<HashMap<ExternalPlanCacheKey, ExternalPlan>>,
}

struct ExternalTableExec {
    props: Arc<PlanProperties>,
    schema: SchemaRef,
    db: Arc<Database>,
    table: Arc<dyn ExternalTable>,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl std::fmt::Debug for ExternalTableExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalTableExec").finish_non_exhaustive()
    }
}

impl DisplayAs for ExternalTableExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "ExternalTableExec")
    }
}

impl ExecutionPlan for ExternalTableExec {
    fn name(&self) -> &str {
        "ExternalTableExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ExternalTableExec is a leaf node and has no children".into(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        task_context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        use futures::TryStreamExt;

        self.db
            .ensure_consistent_read()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ExternalTableExec is single-partition; invalid partition {partition}"
            )));
        }
        let query = task_context
            .session_config()
            .get_extension::<SqlTaskContext>()
            .map(|context| context.query().clone());
        let control = query
            .as_ref()
            .map(|query| query.control().child_with_deadline(None))
            .unwrap_or_else(|| mongreldb_core::ExecutionControl::new(None));
        control
            .checkpoint()
            .map_err(|error| external_control_error(query.as_ref(), error))?;
        let table = Arc::clone(&self.table);
        let projection = self.projection.clone();
        let filters = self.filters.clone();
        let limit = self.limit;
        let worker_control = control.clone();
        let future = async move {
            let mut task = tokio::task::spawn_blocking(move || {
                let schema = table.schema();
                let request = ExternalPlanRequest {
                    projection,
                    filters: filters
                        .iter()
                        .map(|filter| external_filter_from_expr(filter, &schema))
                        .collect(),
                    raw_filters: filters.iter().collect(),
                    order_by: Vec::new(),
                    limit,
                    offset: None,
                };
                table.scan_with_control(&request, &worker_control)
            });
            let scan = tokio::select! {
                result = &mut task => result.map_err(|error| {
                    DataFusionError::Execution(format!("external table worker failed: {error}"))
                })??,
                _ = control.cancelled() => {
                    if tokio::time::timeout(std::time::Duration::from_millis(100), &mut task)
                        .await
                        .is_err()
                    {
                        eprintln!("external table worker exceeded cancellation grace");
                    }
                    return Err(external_control_error_from_control(query.as_ref(), &control));
                }
            };
            control
                .checkpoint()
                .map_err(|error| external_control_error(query.as_ref(), error))?;
            Ok(futures::stream::iter(scan.batches.into_iter().map(Ok)))
        };
        let stream = futures::stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

fn external_control_error(
    query: Option<&crate::RegisteredSqlQuery>,
    error: mongreldb_core::MongrelError,
) -> DataFusionError {
    if let Some(error) = query.and_then(|query| query.checkpoint().err()) {
        DataFusionError::External(Box::new(error))
    } else {
        DataFusionError::Execution(error.to_string())
    }
}

fn external_control_error_from_control(
    query: Option<&crate::RegisteredSqlQuery>,
    control: &mongreldb_core::ExecutionControl,
) -> DataFusionError {
    external_control_error(
        query,
        control
            .checkpoint()
            .expect_err("cancelled external control must fail its checkpoint"),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExternalPlanCacheKey {
    projection: Option<Vec<usize>>,
    filters: Vec<String>,
    order_by: Vec<ExternalOrder>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl ExternalPlanCacheKey {
    fn from_request(request: &ExternalPlanRequest<'_>) -> Self {
        Self {
            projection: request.projection.clone(),
            filters: request
                .filters
                .iter()
                .map(|filter| format!("{filter:?}"))
                .collect(),
            order_by: request.order_by.clone(),
            limit: request.limit,
            offset: request.offset,
        }
    }
}

impl ExternalTableProvider {
    fn new(
        db: Arc<Database>,
        table: Arc<dyn ExternalTable>,
        capabilities: ModuleCapabilities,
    ) -> Self {
        Self {
            db,
            table,
            cache_plans: capabilities.read_only && capabilities.deterministic,
            plan_cache: parking_lot::Mutex::new(HashMap::new()),
        }
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        if !self.cache_plans {
            return self.table.plan_with_control(request, control);
        }
        let key = ExternalPlanCacheKey::from_request(request);
        if let Some(plan) = self.plan_cache.lock().get(&key).cloned() {
            external_df_checkpoint(control)?;
            return Ok(plan);
        }
        let plan = self.table.plan_with_control(request, control)?;
        external_df_checkpoint(control)?;
        self.plan_cache.lock().insert(key, plan.clone());
        Ok(plan)
    }
}

impl std::fmt::Debug for ExternalTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalTableProvider")
            .field("table", &self.table)
            .finish()
    }
}

#[async_trait::async_trait]
impl TableProvider for ExternalTableProvider {
    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        let schema = self.table.schema();
        let request = ExternalPlanRequest {
            projection: None,
            filters: filters
                .iter()
                .map(|filter| external_filter_from_expr(filter, &schema))
                .collect(),
            raw_filters: filters.to_vec(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let query = crate::CURRENT_SQL_QUERY.try_with(Clone::clone).ok();
        let fallback = ExecutionControl::new(None);
        let control = query
            .as_ref()
            .map_or(&fallback, RegisteredSqlQuery::control);
        Ok(self.plan_with_control(&request, control)?.filter_pushdown)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        mongreldb_core::trace::QueryTrace::record(|t| {
            t.scan_mode = mongreldb_core::trace::ScanMode::ExternalModule;
        });
        let schema = self.table.schema();
        let request = ExternalPlanRequest {
            projection: projection.cloned(),
            filters: filters
                .iter()
                .map(|filter| external_filter_from_expr(filter, &schema))
                .collect(),
            raw_filters: filters.iter().collect(),
            order_by: Vec::new(),
            limit,
            offset: None,
        };
        let query = crate::CURRENT_SQL_QUERY.try_with(Clone::clone).ok();
        let fallback = ExecutionControl::new(None);
        let control = query
            .as_ref()
            .map_or(&fallback, RegisteredSqlQuery::control);
        let _plan = self.plan_with_control(&request, control)?;
        let output_schema = match projection {
            Some(projection) => Arc::new(schema.project(projection)?),
            None => schema,
        };
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(ExternalTableExec {
            props,
            schema: output_schema,
            db: Arc::clone(&self.db),
            table: Arc::clone(&self.table),
            projection: projection.cloned(),
            filters: filters.to_vec(),
            limit,
        }))
    }
}

struct JsonModule {
    name: &'static str,
    mode: JsonTableMode,
}

impl ExternalTableModule for JsonModule {
    fn name(&self) -> &str {
        self.name
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        json_descriptor()
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        let (json, root) = json_args(entry, self.name)?;
        let (schema, batches) =
            json_table_batches_from_text(self.name, &json, root.as_deref(), self.mode)
                .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        Ok(Arc::new(JsonExternalTable {
            module: self.name,
            schema,
            batches,
        }))
    }
}

#[derive(Clone)]
struct JsonExternalTable {
    module: &'static str,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl std::fmt::Debug for JsonExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonExternalTable")
            .field("module", &self.module)
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for JsonExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        Ok(ExternalPlan::new(
            request
                .filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect(),
            None,
            1.0,
            false,
        ))
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        project_scan_with_control(
            self.schema.clone(),
            self.batches.clone(),
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

fn json_descriptor() -> ExternalModuleDescriptor {
    ExternalModuleDescriptor {
        schema: CoreSchema {
            schema_id: 0,
            columns: vec![
                json_column(1, "key", TypeId::Bytes, true),
                json_column(2, "value", TypeId::Bytes, true),
                json_column(3, "type", TypeId::Bytes, true),
                json_column(4, "atom", TypeId::Bytes, true),
                json_column(5, "id", TypeId::Int64, false),
                json_column(6, "parent", TypeId::Int64, true),
                json_column(7, "fullkey", TypeId::Bytes, true),
                json_column(8, "path", TypeId::Bytes, true),
                json_column(9, "json", TypeId::Bytes, true),
                json_column(10, "root", TypeId::Bytes, true),
            ],
            indexes: Vec::new(),
            colocation: Vec::new(),
            constraints: Default::default(),
            clustered: false,
        },
        hidden_columns: vec!["json".to_string(), "root".to_string()],
        capabilities: ModuleCapabilities {
            read_only: true,
            deterministic: true,
            trigger_safe: true,
            ..ModuleCapabilities::default()
        },
    }
}

fn json_column(id: u16, name: &str, ty: TypeId, nullable: bool) -> CoreColumnDef {
    CoreColumnDef {
        id,
        name: name.to_string(),
        ty,
        flags: if nullable {
            ColumnFlags::empty().with(ColumnFlags::NULLABLE)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
        embedding_source: None,
    }
}

fn json_args(entry: &ExternalTableEntry, module: &str) -> Result<(String, Option<String>)> {
    match entry.args.as_slice() {
        [json] => Ok((module_arg_string(json).to_string(), None)),
        [json, root] => Ok((
            module_arg_string(json).to_string(),
            Some(module_arg_string(root).to_string()),
        )),
        _ => Err(MongrelQueryError::Schema(format!(
            "{module} external table requires one JSON argument and an optional root path"
        ))),
    }
}

fn module_arg_string(arg: &ModuleArg) -> &str {
    match arg {
        ModuleArg::Ident(value) | ModuleArg::String(value) | ModuleArg::Number(value) => value,
    }
}

struct SchemaTablesModule;

impl ExternalTableModule for SchemaTablesModule {
    fn name(&self) -> &str {
        "schema_tables"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: CoreSchema {
                schema_id: 0,
                columns: vec![
                    catalog_column(1, "schema_name", TypeId::Bytes, false),
                    catalog_column(2, "name", TypeId::Bytes, false),
                    catalog_column(3, "type", TypeId::Bytes, false),
                    catalog_column(4, "ncol", TypeId::Int64, false),
                    catalog_column(5, "module", TypeId::Bytes, true),
                    catalog_column(6, "created_epoch", TypeId::Int64, false),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
            hidden_columns: Vec::new(),
            capabilities: catalog_capabilities(),
        }
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        Ok(Arc::new(SchemaTablesExternalTable {
            db: Arc::clone(ctx.database),
            schema: schema_tables_arrow_schema(),
        }))
    }
}

#[derive(Clone)]
struct SchemaTablesExternalTable {
    db: Arc<Database>,
    schema: SchemaRef,
}

impl std::fmt::Debug for SchemaTablesExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaTablesExternalTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for SchemaTablesExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        unsupported_plan(request)
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        external_checkpoint(Some(control), 0)?;
        let batches = schema_tables_batches(&self.db, self.schema.clone(), Some(control))?;
        project_scan_with_control(
            self.schema.clone(),
            batches,
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

struct DbStatModule;

impl ExternalTableModule for DbStatModule {
    fn name(&self) -> &str {
        "dbstat"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: CoreSchema {
                schema_id: 0,
                columns: vec![
                    catalog_column(1, "name", TypeId::Bytes, false),
                    catalog_column(2, "type", TypeId::Bytes, false),
                    catalog_column(3, "rows", TypeId::Int64, true),
                    catalog_column(4, "runs", TypeId::Int64, true),
                    catalog_column(5, "memtable_rows", TypeId::Int64, true),
                    catalog_column(6, "columns", TypeId::Int64, false),
                    catalog_column(7, "storage_bytes", TypeId::Int64, false),
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
            hidden_columns: Vec::new(),
            capabilities: catalog_capabilities(),
        }
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        Ok(Arc::new(DbStatExternalTable {
            db: Arc::clone(ctx.database),
            schema: dbstat_arrow_schema(),
        }))
    }
}

#[derive(Clone)]
struct DbStatExternalTable {
    db: Arc<Database>,
    schema: SchemaRef,
}

impl std::fmt::Debug for DbStatExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbStatExternalTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for DbStatExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        unsupported_plan(request)
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        external_checkpoint(Some(control), 0)?;
        let batches = dbstat_batches(&self.db, self.schema.clone(), Some(control))?;
        project_scan_with_control(
            self.schema.clone(),
            batches,
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

fn catalog_column(id: u16, name: &str, ty: TypeId, nullable: bool) -> CoreColumnDef {
    CoreColumnDef {
        id,
        name: name.to_string(),
        ty,
        flags: if nullable {
            ColumnFlags::empty().with(ColumnFlags::NULLABLE)
        } else {
            ColumnFlags::empty()
        },
        default_value: None,
        embedding_source: None,
    }
}

fn catalog_capabilities() -> ModuleCapabilities {
    ModuleCapabilities {
        read_only: true,
        trigger_safe: true,
        ..ModuleCapabilities::default()
    }
}

fn ensure_no_args(entry: &ExternalTableEntry, module: &str) -> Result<()> {
    if entry.args.is_empty() {
        Ok(())
    } else {
        Err(MongrelQueryError::Schema(format!(
            "{module} external table does not accept arguments"
        )))
    }
}

fn schema_tables_arrow_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("ncol", DataType::Int64, false),
        Field::new("module", DataType::Utf8, true),
        Field::new("created_epoch", DataType::Int64, false),
    ]))
}

fn schema_tables_batches(
    db: &Arc<Database>,
    schema: SchemaRef,
    control: Option<&mongreldb_core::ExecutionControl>,
) -> DFResult<Vec<RecordBatch>> {
    struct Row {
        name: String,
        ty: String,
        ncol: i64,
        module: Option<String>,
        created_epoch: i64,
    }

    let catalog = db.catalog_snapshot();
    let mut rows = Vec::new();
    for (index, table) in catalog
        .tables
        .into_iter()
        .filter(|table| matches!(table.state, TableState::Live))
        .enumerate()
    {
        external_checkpoint(control, index)?;
        rows.push(Row {
            name: table.name,
            ty: "table".to_string(),
            ncol: saturating_i64(table.schema.columns.len() as u64),
            module: None,
            created_epoch: saturating_i64(table.created_epoch),
        });
    }
    for (index, table) in catalog.external_tables.into_iter().enumerate() {
        external_checkpoint(control, index)?;
        rows.push(Row {
            name: table.name,
            ty: "external".to_string(),
            ncol: saturating_i64(table.declared_schema.columns.len() as u64),
            module: Some(table.module),
            created_epoch: saturating_i64(table.created_epoch),
        });
    }
    rows.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.ty.cmp(&b.ty)));

    let schema_names = vec!["main".to_string(); rows.len()];
    let names = rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>();
    let types = rows.iter().map(|row| row.ty.clone()).collect::<Vec<_>>();
    let ncols = rows.iter().map(|row| row.ncol).collect::<Vec<_>>();
    let modules = rows
        .iter()
        .map(|row| row.module.clone())
        .collect::<Vec<_>>();
    let created_epochs = rows.iter().map(|row| row.created_epoch).collect::<Vec<_>>();

    Ok(vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(schema_names)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(ncols)),
            Arc::new(StringArray::from(modules)),
            Arc::new(Int64Array::from(created_epochs)),
        ],
    )?])
}

fn dbstat_arrow_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new("rows", DataType::Int64, true),
        Field::new("runs", DataType::Int64, true),
        Field::new("memtable_rows", DataType::Int64, true),
        Field::new("columns", DataType::Int64, false),
        Field::new("storage_bytes", DataType::Int64, false),
    ]))
}

fn dbstat_batches(
    db: &Arc<Database>,
    schema: SchemaRef,
    control: Option<&mongreldb_core::ExecutionControl>,
) -> DFResult<Vec<RecordBatch>> {
    struct Row {
        name: String,
        ty: String,
        rows: Option<i64>,
        runs: Option<i64>,
        memtable_rows: Option<i64>,
        columns: i64,
        storage_bytes: i64,
    }

    let catalog = db.catalog_snapshot();
    let mut rows = Vec::new();
    for (index, table) in catalog
        .tables
        .into_iter()
        .filter(|table| matches!(table.state, TableState::Live))
        .enumerate()
    {
        external_checkpoint(control, index)?;
        let handle = db
            .table(&table.name)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let table_guard = handle.lock();
        let table_dir = db.root().join(TABLES_DIR).join(table.table_id.to_string());
        rows.push(Row {
            name: table.name,
            ty: "table".to_string(),
            rows: Some(saturating_i64(table_guard.count())),
            runs: Some(saturating_i64(table_guard.run_count() as u64)),
            memtable_rows: Some(saturating_i64(table_guard.memtable_len() as u64)),
            columns: saturating_i64(table.schema.columns.len() as u64),
            storage_bytes: saturating_i64(dir_size(&table_dir, control)?),
        });
    }
    for (index, table) in catalog.external_tables.into_iter().enumerate() {
        external_checkpoint(control, index)?;
        let state_dir = db.root().join(VTAB_DIR).join(&table.name);
        rows.push(Row {
            name: table.name,
            ty: "external".to_string(),
            rows: None,
            runs: None,
            memtable_rows: None,
            columns: saturating_i64(table.declared_schema.columns.len() as u64),
            storage_bytes: saturating_i64(dir_size(&state_dir, control)?),
        });
    }
    rows.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.ty.cmp(&b.ty)));

    let names = rows.iter().map(|row| row.name.clone()).collect::<Vec<_>>();
    let types = rows.iter().map(|row| row.ty.clone()).collect::<Vec<_>>();
    let row_counts = rows.iter().map(|row| row.rows).collect::<Vec<_>>();
    let runs = rows.iter().map(|row| row.runs).collect::<Vec<_>>();
    let memtable_rows = rows.iter().map(|row| row.memtable_rows).collect::<Vec<_>>();
    let columns = rows.iter().map(|row| row.columns).collect::<Vec<_>>();
    let storage_bytes = rows.iter().map(|row| row.storage_bytes).collect::<Vec<_>>();

    Ok(vec![RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(row_counts)),
            Arc::new(Int64Array::from(runs)),
            Arc::new(Int64Array::from(memtable_rows)),
            Arc::new(Int64Array::from(columns)),
            Arc::new(Int64Array::from(storage_bytes)),
        ],
    )?])
}

fn dir_size(path: &Path, control: Option<&mongreldb_core::ExecutionControl>) -> DFResult<u64> {
    external_checkpoint(control, 0)?;
    if !path.exists() {
        return Ok(0);
    }
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|e| DataFusionError::Execution(format!("stat {:?}: {e}", path)))?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0_u64;
    for (index, entry) in std::fs::read_dir(path)
        .map_err(|e| DataFusionError::Execution(format!("{e}")))?
        .enumerate()
    {
        external_checkpoint(control, index)?;
        let entry = entry.map_err(|e| DataFusionError::Execution(format!("{e}")))?;
        total = total.saturating_add(dir_size(&entry.path(), control)?);
    }
    Ok(total)
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unsupported_plan(request: &ExternalPlanRequest<'_>) -> DFResult<ExternalPlan> {
    Ok(ExternalPlan::new(
        request
            .filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect(),
        None,
        1.0,
        false,
    ))
}

#[cfg(test)]
fn project_scan(
    full_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    projection: Option<&[usize]>,
    limit: Option<usize>,
) -> DFResult<ExternalScan> {
    project_scan_with_control(full_schema, batches, projection, limit, None)
}

fn project_scan_with_control(
    full_schema: SchemaRef,
    batches: Vec<RecordBatch>,
    projection: Option<&[usize]>,
    limit: Option<usize>,
    control: Option<&mongreldb_core::ExecutionControl>,
) -> DFResult<ExternalScan> {
    external_checkpoint(control, 0)?;
    let Some(projection) = projection else {
        return Ok(ExternalScan {
            schema: full_schema,
            batches: limit_batches(batches, limit, control)?,
        });
    };
    let schema = Arc::new(ArrowSchema::new(
        projection
            .iter()
            .map(|idx| full_schema.field(*idx).clone())
            .collect::<Vec<_>>(),
    ));
    let projected = batches
        .into_iter()
        .enumerate()
        .map(|(index, batch)| {
            external_checkpoint(control, index)?;
            let columns = projection
                .iter()
                .map(|idx| batch.column(*idx).clone())
                .collect::<Vec<_>>();
            let batch = if columns.is_empty() {
                RecordBatch::try_new_with_options(
                    schema.clone(),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )
            } else {
                RecordBatch::try_new(schema.clone(), columns)
            };
            batch.map_err(DataFusionError::from)
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(ExternalScan {
        schema,
        batches: limit_batches(projected, limit, control)?,
    })
}

fn limit_batches(
    batches: Vec<RecordBatch>,
    limit: Option<usize>,
    control: Option<&mongreldb_core::ExecutionControl>,
) -> DFResult<Vec<RecordBatch>> {
    let Some(mut remaining) = limit else {
        return Ok(batches);
    };
    let mut limited = Vec::new();
    for (index, batch) in batches.into_iter().enumerate() {
        external_checkpoint(control, index)?;
        if remaining == 0 {
            break;
        }
        let take = remaining.min(batch.num_rows());
        limited.push(batch.slice(0, take));
        remaining -= take;
    }
    Ok(limited)
}

#[inline]
fn external_checkpoint(
    control: Option<&mongreldb_core::ExecutionControl>,
    index: usize,
) -> DFResult<()> {
    if index.is_multiple_of(256) {
        control
            .map(mongreldb_core::ExecutionControl::checkpoint)
            .transpose()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct BlockingExternalModule {
        entered: Arc<std::sync::Barrier>,
    }

    impl BlockingExternalModule {
        fn block(&self, control: &ExecutionControl) -> Result<()> {
            self.entered.wait();
            loop {
                control.checkpoint()?;
                std::thread::yield_now();
            }
        }
    }

    impl ExternalTableModule for BlockingExternalModule {
        fn name(&self) -> &str {
            "blocking"
        }

        fn descriptor(&self) -> ExternalModuleDescriptor {
            ExternalModuleDescriptor {
                schema: CoreSchema::default(),
                hidden_columns: Vec::new(),
                capabilities: ModuleCapabilities::default(),
            }
        }

        fn indexes_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _entry: &ExternalTableEntry,
        ) -> Result<Vec<ExternalModuleIndex>> {
            self.block(context.control)?;
            unreachable!()
        }

        fn connect_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _entry: &ExternalTableEntry,
        ) -> Result<Arc<dyn ExternalTable>> {
            self.block(context.control)?;
            unreachable!()
        }

        fn read_rows_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _entry: &ExternalTableEntry,
        ) -> Result<Vec<HashMap<u16, Value>>> {
            self.block(context.control)?;
            unreachable!()
        }

        fn prepare_rows_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _entry: &ExternalTableEntry,
            _rows: Vec<HashMap<u16, Value>>,
        ) -> Result<Vec<u8>> {
            self.block(context.control)?;
            unreachable!()
        }

        fn rows_from_state_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _state: &[u8],
        ) -> Result<Vec<HashMap<u16, Value>>> {
            self.block(context.control)?;
            unreachable!()
        }

        fn write_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _entry: &ExternalTableEntry,
            _op: ExternalWriteOp,
            _txn: &mut ExternalTxn,
        ) -> Result<ExternalWriteResult> {
            self.block(context.control)?;
            unreachable!()
        }

        fn destroy_with_control(
            &self,
            context: &ExternalExecutionContext<'_>,
            _entry: &ExternalTableEntry,
        ) -> Result<()> {
            self.block(context.control)
        }
    }

    impl ExternalTable for BlockingExternalModule {
        fn schema(&self) -> SchemaRef {
            Arc::new(ArrowSchema::empty())
        }

        fn plan_with_control(
            &self,
            _request: &ExternalPlanRequest<'_>,
            control: &ExecutionControl,
        ) -> DFResult<ExternalPlan> {
            self.block(control)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            unreachable!()
        }

        fn scan_with_control(
            &self,
            _request: &ExternalPlanRequest<'_>,
            control: &ExecutionControl,
        ) -> DFResult<ExternalScan> {
            self.block(control)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            unreachable!()
        }
    }

    fn cancel_blocking_callback<R>(
        database: &Arc<Database>,
        entry: &ExternalTableEntry,
        callback: impl FnOnce(
            &BlockingExternalModule,
            &ExternalExecutionContext<'_>,
            &ExternalTableEntry,
        ) -> R,
    ) -> R {
        let entered = Arc::new(std::sync::Barrier::new(2));
        let module = BlockingExternalModule {
            entered: Arc::clone(&entered),
        };
        let control = ExecutionControl::new(None);
        let cancel_control = control.clone();
        let canceller = std::thread::spawn(move || {
            entered.wait();
            cancel_control.cancel(mongreldb_core::CancellationReason::ClientRequest);
        });
        let context = ExternalExecutionContext {
            database,
            control: &control,
            query_id: None,
        };
        let result = callback(&module, &context, entry);
        canceller.join().unwrap();
        result
    }

    #[test]
    fn every_external_callback_can_observe_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        let database = Arc::new(Database::create(dir.path()).unwrap());
        let entry = ExternalTableEntry {
            name: "blocked".into(),
            module: "blocking".into(),
            args: Vec::new(),
            declared_schema: CoreSchema::default(),
            hidden_columns: Vec::new(),
            options: BTreeMap::new(),
            capabilities: ModuleCapabilities::default(),
            created_epoch: 0,
        };

        let cancelled = |error: &MongrelQueryError| {
            matches!(
                error,
                MongrelQueryError::Core(mongreldb_core::MongrelError::Cancelled)
            )
        };
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, entry| {
                module.indexes_with_control(context, entry)
            })
            .unwrap_err()
        ));
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, entry| {
                module.connect_with_control(context, entry)
            })
            .unwrap_err()
        ));
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, entry| {
                module.read_rows_with_control(context, entry)
            })
            .unwrap_err()
        ));
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, entry| {
                module.prepare_rows_with_control(context, entry, Vec::new())
            })
            .unwrap_err()
        ));
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, _| {
                module.rows_from_state_with_control(context, &[])
            })
            .unwrap_err()
        ));
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, entry| {
                module.write_with_control(
                    context,
                    entry,
                    ExternalWriteOp::Insert { rows: Vec::new() },
                    &mut ExternalTxn::new(Vec::new()),
                )
            })
            .unwrap_err()
        ));
        assert!(cancelled(
            &cancel_blocking_callback(&database, &entry, |module, context, entry| {
                module.destroy_with_control(context, entry)
            })
            .unwrap_err()
        ));

        let request = ExternalPlanRequest {
            projection: None,
            filters: Vec::new(),
            raw_filters: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };
        let plan_result = cancel_blocking_callback(&database, &entry, |module, context, _| {
            module.plan_with_control(&request, context.control)
        });
        assert!(matches!(
            plan_result,
            Err(error) if error.to_string().contains("cancelled")
        ));
    }

    #[test]
    fn project_scan_applies_limit_after_projection() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 11, 12, 13])) as ArrayRef,
            ],
        )
        .unwrap();

        let scan = project_scan(schema, vec![batch], Some(&[1]), Some(2)).unwrap();

        assert_eq!(scan.schema.fields().len(), 1);
        assert_eq!(scan.schema.field(0).name(), "value");
        assert_eq!(scan.batches.len(), 1);
        assert_eq!(scan.batches[0].num_columns(), 1);
        assert_eq!(scan.batches[0].num_rows(), 2);
        let values = scan.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 11);
    }

    #[test]
    fn external_plan_derives_accepted_and_residual_filter_metadata() {
        let plan = ExternalPlan::new(
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Unsupported,
            ],
            Some(42),
            3.5,
            true,
        );

        assert_eq!(
            plan.accepted_filters,
            vec![
                AcceptedFilter {
                    filter_index: 0,
                    pushdown: TableProviderFilterPushDown::Exact,
                },
                AcceptedFilter {
                    filter_index: 1,
                    pushdown: TableProviderFilterPushDown::Inexact,
                },
            ]
        );
        assert!(plan.residual_filters_required);
        assert_eq!(plan.estimated_rows, Some(42));
        assert_eq!(plan.estimated_cost, 3.5);
        assert!(plan.order_satisfied);
    }

    #[test]
    fn external_state_writer_stops_before_over_budget_allocation() {
        use std::io::Write as _;

        let mut writer = LimitedStateWriter::new(&[]);
        writer.requested = EXTERNAL_STATE_BYTES_LIMIT;
        assert!(writer.write_all(b"x").is_err());
        assert!(writer.bytes.is_empty());
    }

    #[test]
    fn external_base_write_budget_rejects_before_push() {
        let mut txn = ExternalTxn::new(Vec::new());
        txn.base_write_bytes = EXTERNAL_BASE_WRITE_BYTES_LIMIT;
        let error = txn
            .emit_base_write(ExternalBaseWrite::Delete {
                table: "t".into(),
                row_id: 1,
            })
            .unwrap_err();
        assert!(matches!(
            error,
            MongrelQueryError::Core(mongreldb_core::MongrelError::ResourceLimitExceeded { .. })
        ));
        assert!(txn.base_writes.is_empty());
    }
}

struct KvStoreModule;

impl ExternalTableModule for KvStoreModule {
    fn name(&self) -> &str {
        "kv_store"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: kv_store_schema(),
            hidden_columns: Vec::new(),
            capabilities: ModuleCapabilities {
                writable: true,
                deterministic: true,
                trigger_safe: true,
                transaction_safe: true,
                ..ModuleCapabilities::default()
            },
        }
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        let rows = read_state_rows(ctx.database, entry)?;
        let schema = arrow_conv::arrow_schema(&entry.declared_schema)?;
        let batches = core_rows_to_batches(&entry.declared_schema, rows)?;
        Ok(Arc::new(KvStoreExternalTable { schema, batches }))
    }

    fn read_rows_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Vec<HashMap<u16, Value>>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        read_state_rows(ctx.database, entry)
    }

    fn prepare_rows_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
        rows: Vec<HashMap<u16, Value>>,
    ) -> Result<Vec<u8>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        validate_external_rows(&entry.declared_schema, &rows)?;
        encode_state_rows(&rows)
    }
}

#[derive(Clone)]
struct KvStoreExternalTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl std::fmt::Debug for KvStoreExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvStoreExternalTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for KvStoreExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        unsupported_plan(request)
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        project_scan_with_control(
            self.schema.clone(),
            self.batches.clone(),
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

fn kv_store_schema() -> CoreSchema {
    CoreSchema {
        schema_id: 0,
        columns: vec![
            CoreColumnDef {
                id: 1,
                name: "key".to_string(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            CoreColumnDef {
                id: 2,
                name: "value".to_string(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

#[derive(Serialize, Deserialize)]
struct ExternalRowState {
    cells: Vec<(u16, Value)>,
}

const KV_STATE_MAGIC: &[u8] = b"mongreldb.external.kv.v1\n";

#[derive(Serialize, Deserialize)]
struct ExternalKvState {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

fn state_file(db: &Arc<Database>, entry: &ExternalTableEntry) -> PathBuf {
    db.root()
        .join(VTAB_DIR)
        .join(&entry.name)
        .join("state.json")
}

pub(crate) fn external_table_state_bytes(
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
) -> Result<Vec<u8>> {
    db.ensure_consistent_read()?;
    let path = state_file(db, entry);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let metadata = fs::metadata(&path)
        .map_err(|e| MongrelQueryError::Schema(format!("stat external state {:?}: {e}", path)))?;
    if metadata.len() > EXTERNAL_STATE_BYTES_LIMIT as u64 {
        return Err(external_resource_limit(
            "external table state file bytes",
            usize::try_from(metadata.len()).unwrap_or(usize::MAX),
            EXTERNAL_STATE_BYTES_LIMIT,
        ));
    }
    let state = fs::read(&path)
        .map_err(|e| MongrelQueryError::Schema(format!("read external state {:?}: {e}", path)))?;
    enforce_external_state_limit(&state)?;
    Ok(state)
}

fn read_state_rows(
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
) -> Result<Vec<HashMap<u16, Value>>> {
    let bytes = external_table_state_bytes(db, entry)?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    decode_state_rows(&bytes)
}

struct LimitedStateWriter {
    bytes: Vec<u8>,
    requested: usize,
}

impl LimitedStateWriter {
    fn new(prefix: &[u8]) -> Self {
        Self {
            bytes: prefix.to_vec(),
            requested: prefix.len(),
        }
    }
}

impl std::io::Write for LimitedStateWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.requested = self.requested.saturating_add(buf.len());
        if self.requested > EXTERNAL_STATE_BYTES_LIMIT {
            return Err(std::io::Error::other("external state byte limit exceeded"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_external_json<T: Serialize + ?Sized>(value: &T, prefix: &[u8]) -> Result<Vec<u8>> {
    let mut writer = LimitedStateWriter::new(prefix);
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        if writer.requested > EXTERNAL_STATE_BYTES_LIMIT {
            return Err(external_resource_limit(
                "external table encoded state bytes",
                writer.requested,
                EXTERNAL_STATE_BYTES_LIMIT,
            ));
        }
        return Err(MongrelQueryError::Schema(format!(
            "encode external state: {error}"
        )));
    }
    Ok(writer.bytes)
}

fn encode_state_rows(rows: &[HashMap<u16, Value>]) -> Result<Vec<u8>> {
    enforce_external_rows_limit(rows, None)?;
    let state = rows
        .iter()
        .map(|row| {
            let mut cells = row
                .iter()
                .map(|(id, value)| (*id, value.clone()))
                .collect::<Vec<_>>();
            cells.sort_by_key(|(id, _)| *id);
            ExternalRowState { cells }
        })
        .collect::<Vec<_>>();
    encode_external_json(&state, &[])
}

fn decode_state_rows(state: &[u8]) -> Result<Vec<HashMap<u16, Value>>> {
    enforce_external_state_limit(state)?;
    let rows: Vec<ExternalRowState> = serde_json::from_slice(state)
        .map_err(|e| MongrelQueryError::Schema(format!("decode external state: {e}")))?;
    let rows = rows
        .into_iter()
        .map(|row| row.cells.into_iter().collect())
        .collect::<Vec<_>>();
    enforce_external_rows_limit(&rows, None)?;
    Ok(rows)
}

fn decode_kv_state(state: &[u8]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    enforce_external_state_limit(state)?;
    if state.is_empty() {
        return Ok(BTreeMap::new());
    }
    let json = state.strip_prefix(KV_STATE_MAGIC).ok_or_else(|| {
        MongrelQueryError::Schema(
            "external transaction state is not in key/value module format".into(),
        )
    })?;
    let decoded: ExternalKvState = serde_json::from_slice(json)
        .map_err(|e| MongrelQueryError::Schema(format!("decode external kv state: {e}")))?;
    if decoded.entries.len() > EXTERNAL_ROWS_LIMIT {
        return Err(external_resource_limit(
            "external transaction key/value count",
            decoded.entries.len(),
            EXTERNAL_ROWS_LIMIT,
        ));
    }
    let bytes = decoded.entries.iter().fold(0_usize, |total, (key, value)| {
        total.saturating_add(key.len()).saturating_add(value.len())
    });
    if bytes > EXTERNAL_STATE_BYTES_LIMIT {
        return Err(external_resource_limit(
            "external transaction key/value bytes",
            bytes,
            EXTERNAL_STATE_BYTES_LIMIT,
        ));
    }
    Ok(decoded.entries.into_iter().collect())
}

fn encode_kv_state(state: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<Vec<u8>> {
    if state.len() > EXTERNAL_ROWS_LIMIT {
        return Err(external_resource_limit(
            "external transaction key/value count",
            state.len(),
            EXTERNAL_ROWS_LIMIT,
        ));
    }
    let bytes = state.iter().fold(0_usize, |total, (key, value)| {
        total.saturating_add(key.len()).saturating_add(value.len())
    });
    if bytes > EXTERNAL_STATE_BYTES_LIMIT {
        return Err(external_resource_limit(
            "external transaction key/value bytes",
            bytes,
            EXTERNAL_STATE_BYTES_LIMIT,
        ));
    }
    let encoded = ExternalKvState {
        entries: state
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    };
    encode_external_json(&encoded, KV_STATE_MAGIC)
}

fn validate_external_rows(schema: &CoreSchema, rows: &[HashMap<u16, Value>]) -> Result<()> {
    enforce_external_rows_limit(rows, None)?;
    let mut known = HashSet::new();
    let mut pk_cols = Vec::new();
    for column in &schema.columns {
        known.insert(column.id);
        if column.flags.contains(ColumnFlags::PRIMARY_KEY) {
            pk_cols.push(column.id);
        }
    }
    let mut keys = HashSet::new();
    for row in rows {
        for column_id in row.keys() {
            if !known.contains(column_id) {
                return Err(MongrelQueryError::Schema(format!(
                    "external row contains unknown column id {column_id}"
                )));
            }
        }
        if !pk_cols.is_empty() {
            let mut key = Vec::new();
            for column_id in &pk_cols {
                let value = row.get(column_id).ok_or_else(|| {
                    MongrelQueryError::Schema(format!(
                        "external row is missing primary key column {column_id}"
                    ))
                })?;
                key.extend_from_slice(&column_id.to_be_bytes());
                key.extend_from_slice(&value.encode_key());
                key.push(0);
            }
            if !keys.insert(key) {
                return Err(MongrelQueryError::Schema(
                    "external table primary key conflict".into(),
                ));
            }
        }
    }
    Ok(())
}

fn core_rows_to_batches(
    schema: &CoreSchema,
    rows: Vec<HashMap<u16, Value>>,
) -> Result<Vec<RecordBatch>> {
    let arrow_schema = arrow_conv::arrow_schema(schema)?;
    let arrays = schema
        .columns
        .iter()
        .map(|column| {
            let values = rows
                .iter()
                .map(|row| row.get(&column.id).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>();
            arrow_conv::build_array(column.ty.clone(), &values)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(vec![RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?])
}

struct FtsDocsModule;

impl ExternalTableModule for FtsDocsModule {
    fn name(&self) -> &str {
        "fts_docs"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: fts_docs_schema(),
            hidden_columns: vec!["query".to_string()],
            capabilities: ModuleCapabilities {
                writable: true,
                deterministic: true,
                trigger_safe: true,
                ..ModuleCapabilities::default()
            },
        }
    }

    fn indexes_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Vec<ExternalModuleIndex>> {
        context.control.checkpoint()?;
        Ok(vec![ExternalModuleIndex::new(
            format!("{}_fts_inverted", entry.name),
            vec![2],
        )])
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        let options = fts_options(entry)?;
        let rows = read_state_rows(ctx.database, entry)?;
        let index = FtsInvertedIndex::build(&rows, &options);
        Ok(Arc::new(FtsDocsExternalTable {
            schema: arrow_conv::arrow_schema(&entry.declared_schema)?,
            core_schema: entry.declared_schema.clone(),
            options,
            index,
            rows,
        }))
    }

    fn read_rows_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Vec<HashMap<u16, Value>>> {
        ctx.control.checkpoint()?;
        let _ = fts_options(entry)?;
        read_state_rows(ctx.database, entry)
    }

    fn prepare_rows_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
        rows: Vec<HashMap<u16, Value>>,
    ) -> Result<Vec<u8>> {
        ctx.control.checkpoint()?;
        let _ = fts_options(entry)?;
        let rows = rows
            .into_iter()
            .map(|mut row| {
                row.remove(&3);
                row.remove(&4);
                row.remove(&5);
                row.remove(&6);
                row
            })
            .collect::<Vec<_>>();
        validate_external_rows(&entry.declared_schema, &rows)?;
        encode_state_rows(&rows)
    }
}

#[derive(Clone)]
struct FtsDocsExternalTable {
    schema: SchemaRef,
    core_schema: CoreSchema,
    options: FtsOptions,
    index: FtsInvertedIndex,
    rows: Vec<HashMap<u16, Value>>,
}

impl std::fmt::Debug for FtsDocsExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsDocsExternalTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for FtsDocsExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        Ok(ExternalPlan::new(
            request
                .raw_filters
                .iter()
                .map(|filter| {
                    if fts_query_from_expr(filter, &self.options).is_some() {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                })
                .collect(),
            None,
            1.0,
            false,
        ))
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        external_checkpoint(Some(control), 0)?;
        let query = request
            .raw_filters
            .iter()
            .filter_map(|filter| fts_query_from_expr(filter, &self.options))
            .reduce(FtsQuery::and);
        let mut rows = Vec::new();
        if let Some(query) = query.as_ref() {
            for (index, row_index) in self.index.candidates(query).into_iter().enumerate() {
                external_checkpoint(Some(control), index)?;
                let Some(row) = self.rows.get(row_index) else {
                    continue;
                };
                let Some(score) = fts_match_score(row, query, &self.options) else {
                    continue;
                };
                rows.push(fts_enrich_row(
                    row.clone(),
                    Some(query),
                    &self.options,
                    Some(score),
                ));
            }
        } else {
            rows.reserve(self.rows.len());
            for (index, row) in self.rows.iter().cloned().enumerate() {
                external_checkpoint(Some(control), index)?;
                rows.push(fts_enrich_row(row, None, &self.options, None));
            }
        }
        external_checkpoint(Some(control), 0)?;
        project_scan_with_control(
            self.schema.clone(),
            core_rows_to_batches(&self.core_schema, rows)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

fn fts_docs_schema() -> CoreSchema {
    CoreSchema {
        schema_id: 0,
        columns: vec![
            CoreColumnDef {
                id: 1,
                name: "doc_id".to_string(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            CoreColumnDef {
                id: 2,
                name: "text".to_string(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            CoreColumnDef {
                id: 3,
                name: "query".to_string(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            CoreColumnDef {
                id: 4,
                name: "rank".to_string(),
                ty: TypeId::Float64,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            CoreColumnDef {
                id: 5,
                name: "snippet".to_string(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
            CoreColumnDef {
                id: 6,
                name: "highlight".to_string(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

#[derive(Clone)]
struct FtsOptions {
    prefix_queries: bool,
    case_sensitive: bool,
    min_token_len: usize,
    stopwords: HashSet<String>,
}

impl Default for FtsOptions {
    fn default() -> Self {
        Self {
            prefix_queries: false,
            case_sensitive: false,
            min_token_len: 1,
            stopwords: HashSet::new(),
        }
    }
}

impl FtsOptions {
    fn normalize(&self, value: &str) -> String {
        if self.case_sensitive {
            value.to_string()
        } else {
            value.to_ascii_lowercase()
        }
    }

    fn keep_term(&self, term: &str) -> bool {
        term.len() >= self.min_token_len && !self.stopwords.contains(term)
    }
}

#[derive(Clone)]
struct FtsQuery {
    groups: Vec<Vec<FtsClause>>,
    prohibited: Vec<FtsClause>,
}

impl FtsQuery {
    fn and(self, other: Self) -> Self {
        let left_groups = if self.groups.is_empty() {
            vec![Vec::new()]
        } else {
            self.groups
        };
        let right_groups = if other.groups.is_empty() {
            vec![Vec::new()]
        } else {
            other.groups
        };
        let mut groups = Vec::new();
        for left in &left_groups {
            for right in &right_groups {
                let mut combined = left.clone();
                combined.extend(right.clone());
                groups.push(combined);
            }
        }
        let mut prohibited = self.prohibited;
        prohibited.extend(other.prohibited);
        Self { groups, prohibited }
    }

    fn positive_clauses(&self) -> impl Iterator<Item = &FtsClause> {
        self.groups.iter().flatten()
    }
}

#[derive(Clone)]
struct FtsInvertedIndex {
    terms: HashMap<String, Vec<usize>>,
    all_rows: Vec<usize>,
}

impl FtsInvertedIndex {
    fn build(rows: &[HashMap<u16, Value>], options: &FtsOptions) -> Self {
        let mut term_sets: HashMap<String, BTreeSet<usize>> = HashMap::new();
        for (idx, row) in rows.iter().enumerate() {
            let Some(Value::Bytes(text)) = row.get(&2) else {
                continue;
            };
            let text = String::from_utf8_lossy(text);
            let mut row_terms = HashSet::new();
            for span in token_spans_with_options(&text, options) {
                if row_terms.insert(span.term.clone()) {
                    term_sets.entry(span.term).or_default().insert(idx);
                }
            }
        }
        let terms = term_sets
            .into_iter()
            .map(|(term, rows)| (term, rows.into_iter().collect()))
            .collect();
        Self {
            terms,
            all_rows: (0..rows.len()).collect(),
        }
    }

    fn candidates(&self, query: &FtsQuery) -> Vec<usize> {
        let mut rows = if query.groups.is_empty() {
            self.all_rows.iter().copied().collect::<BTreeSet<_>>()
        } else {
            query
                .groups
                .iter()
                .filter_map(|group| self.group_candidates(group))
                .flatten()
                .collect::<BTreeSet<_>>()
        };
        for clause in &query.prohibited {
            for idx in self.clause_candidates(clause) {
                rows.remove(&idx);
            }
        }
        rows.into_iter().collect()
    }

    fn group_candidates(&self, group: &[FtsClause]) -> Option<Vec<usize>> {
        let mut iter = group.iter().map(|clause| self.clause_candidates(clause));
        let first = iter.next()?;
        let mut current = first.into_iter().collect::<BTreeSet<_>>();
        for rows in iter {
            let next = rows.into_iter().collect::<BTreeSet<_>>();
            current = current.intersection(&next).copied().collect();
            if current.is_empty() {
                break;
            }
        }
        Some(current.into_iter().collect())
    }

    fn clause_candidates(&self, clause: &FtsClause) -> Vec<usize> {
        match clause {
            FtsClause::Term { term, prefix } => {
                if *prefix {
                    self.terms
                        .iter()
                        .filter(|(indexed, _)| indexed.starts_with(term))
                        .flat_map(|(_, rows)| rows.iter().copied())
                        .collect::<BTreeSet<_>>()
                        .into_iter()
                        .collect()
                } else {
                    self.terms.get(term).cloned().unwrap_or_default()
                }
            }
            FtsClause::Phrase(terms) => {
                let clauses = terms
                    .iter()
                    .map(|term| FtsClause::Term {
                        term: term.clone(),
                        prefix: false,
                    })
                    .collect::<Vec<_>>();
                self.group_candidates(&clauses).unwrap_or_default()
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
enum FtsClause {
    Term { term: String, prefix: bool },
    Phrase(Vec<String>),
}

enum FtsQueryToken {
    Clause(FtsClause),
    Or,
    Not,
}

fn fts_options(entry: &ExternalTableEntry) -> Result<FtsOptions> {
    let mut options = FtsOptions::default();
    for arg in &entry.args {
        let raw = module_arg_string(arg).trim();
        if raw.is_empty() {
            continue;
        }
        let (key, value) = raw
            .split_once('=')
            .map_or((raw, "true"), |(key, value)| (key.trim(), value.trim()));
        match key.to_ascii_lowercase().as_str() {
            "tokenizer" => match value.to_ascii_lowercase().as_str() {
                "simple" | "ascii" | "unicode61" => {}
                other => {
                    return Err(MongrelQueryError::Schema(format!(
                        "fts_docs tokenizer {other:?} is not supported"
                    )))
                }
            },
            "prefix" | "prefix_queries" => options.prefix_queries = parse_bool_option(value)?,
            "case_sensitive" => options.case_sensitive = parse_bool_option(value)?,
            "min_token_len" => {
                options.min_token_len = value.parse::<usize>().map_err(|e| {
                    MongrelQueryError::Schema(format!(
                        "fts_docs min_token_len {value:?} must be an integer: {e}"
                    ))
                })?;
                if options.min_token_len == 0 {
                    return Err(MongrelQueryError::Schema(
                        "fts_docs min_token_len must be at least 1".into(),
                    ));
                }
            }
            "stopwords" => {
                options.stopwords = value
                    .split('|')
                    .map(str::trim)
                    .filter(|word| !word.is_empty())
                    .map(|word| options.normalize(word))
                    .collect();
            }
            other => {
                return Err(MongrelQueryError::Schema(format!(
                    "fts_docs option {other:?} is not supported"
                )))
            }
        }
    }
    Ok(options)
}

fn parse_bool_option(value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(MongrelQueryError::Schema(format!(
            "expected boolean option value, got {value:?}"
        ))),
    }
}

fn fts_query_from_expr(expr: &Expr, options: &FtsOptions) -> Option<FtsQuery> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            Some(fts_query_from_expr(left, options)?.and(fts_query_from_expr(right, options)?))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            let literal = match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(literal, _)) if column.name == "query" => {
                    literal_string(literal)?
                }
                (Expr::Literal(literal, _), Expr::Column(column)) if column.name == "query" => {
                    literal_string(literal)?
                }
                _ => return None,
            };
            parse_fts_query(&literal, options)
        }
        _ => None,
    }
}

fn parse_fts_query(input: &str, options: &FtsOptions) -> Option<FtsQuery> {
    let tokens = fts_query_tokens(input, options);
    if tokens.is_empty() {
        return None;
    }
    let mut groups: Vec<Vec<FtsClause>> = vec![Vec::new()];
    let mut prohibited = Vec::new();
    let mut negate = false;
    for token in tokens {
        match token {
            FtsQueryToken::Or => {
                if groups.last().is_some_and(|group| !group.is_empty()) {
                    groups.push(Vec::new());
                }
                negate = false;
            }
            FtsQueryToken::Not => negate = true,
            FtsQueryToken::Clause(clause) => {
                if negate {
                    prohibited.push(clause);
                    negate = false;
                } else if let Some(group) = groups.last_mut() {
                    group.push(clause);
                }
            }
        }
    }
    groups.retain(|group| !group.is_empty());
    if groups.is_empty() && prohibited.is_empty() {
        None
    } else {
        Some(FtsQuery { groups, prohibited })
    }
}

fn fts_query_tokens(input: &str, options: &FtsOptions) -> Vec<FtsQueryToken> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((idx, ch)) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '"' {
            let start = idx + ch.len_utf8();
            let mut end = input.len();
            for (next_idx, next_ch) in chars.by_ref() {
                if next_ch == '"' {
                    end = next_idx;
                    break;
                }
            }
            let terms = token_spans_with_options(&input[start..end], options)
                .into_iter()
                .map(|span| span.term)
                .collect::<Vec<_>>();
            if !terms.is_empty() {
                tokens.push(FtsQueryToken::Clause(FtsClause::Phrase(terms)));
            }
            continue;
        }
        let start = idx;
        let mut end = idx + ch.len_utf8();
        while let Some((next_idx, next_ch)) = chars.peek().copied() {
            if next_ch.is_whitespace() {
                break;
            }
            chars.next();
            end = next_idx + next_ch.len_utf8();
        }
        let raw = &input[start..end];
        if raw.eq_ignore_ascii_case("OR") {
            tokens.push(FtsQueryToken::Or);
        } else if raw.eq_ignore_ascii_case("NOT") {
            tokens.push(FtsQueryToken::Not);
        } else if let Some(rest) = raw.strip_prefix('-') {
            tokens.push(FtsQueryToken::Not);
            tokens.extend(
                word_clauses(rest, options)
                    .into_iter()
                    .map(FtsQueryToken::Clause),
            );
        } else {
            tokens.extend(
                word_clauses(raw, options)
                    .into_iter()
                    .map(FtsQueryToken::Clause),
            );
        }
    }
    tokens
}

fn word_clauses(raw: &str, options: &FtsOptions) -> Vec<FtsClause> {
    let raw = raw.trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '*');
    if raw.is_empty() {
        return Vec::new();
    }
    let prefix = options.prefix_queries && raw.ends_with('*');
    let raw = raw.trim_end_matches('*');
    token_spans_with_options(raw, options)
        .into_iter()
        .map(|span| FtsClause::Term {
            term: span.term,
            prefix,
        })
        .collect()
}

fn fts_match_score(
    row: &HashMap<u16, Value>,
    query: &FtsQuery,
    options: &FtsOptions,
) -> Option<f64> {
    let Some(Value::Bytes(text)) = row.get(&2) else {
        return None;
    };
    let text = String::from_utf8_lossy(text);
    let tokens = token_spans_with_options(&text, options);
    if query
        .prohibited
        .iter()
        .any(|clause| clause_matches(&tokens, clause))
    {
        return None;
    }
    let positive_score = if query.groups.is_empty() {
        Some(0.0)
    } else {
        query
            .groups
            .iter()
            .filter(|group| group.iter().all(|clause| clause_matches(&tokens, clause)))
            .map(|group| {
                group
                    .iter()
                    .map(|clause| clause_score(&tokens, clause))
                    .sum()
            })
            .max_by(|left: &f64, right: &f64| left.total_cmp(right))
    }?;
    Some(positive_score)
}

fn clause_matches(tokens: &[TokenSpan], clause: &FtsClause) -> bool {
    clause_score(tokens, clause) > 0.0
}

fn clause_score(tokens: &[TokenSpan], clause: &FtsClause) -> f64 {
    match clause {
        FtsClause::Term { term, prefix } => tokens
            .iter()
            .filter(|token| token_matches_term(&token.term, term, *prefix))
            .count() as f64,
        FtsClause::Phrase(terms) => phrase_occurrences(tokens, terms) as f64 * terms.len() as f64,
    }
}

fn token_matches_term(token: &str, term: &str, prefix: bool) -> bool {
    if prefix {
        token.starts_with(term)
    } else {
        token == term
    }
}

fn phrase_occurrences(tokens: &[TokenSpan], terms: &[String]) -> usize {
    if terms.is_empty() || tokens.len() < terms.len() {
        return 0;
    }
    tokens
        .windows(terms.len())
        .filter(|window| {
            window
                .iter()
                .zip(terms)
                .all(|(token, term)| token.term == *term)
        })
        .count()
}

fn token_matches_positive_clause(token: &str, query: &FtsQuery) -> bool {
    query.positive_clauses().any(|clause| match clause {
        FtsClause::Term { term, prefix } => token_matches_term(token, term, *prefix),
        FtsClause::Phrase(terms) => terms.iter().any(|term| term == token),
    })
}

fn fts_enrich_row(
    mut row: HashMap<u16, Value>,
    query: Option<&FtsQuery>,
    options: &FtsOptions,
    score: Option<f64>,
) -> HashMap<u16, Value> {
    row.remove(&3);
    row.remove(&4);
    row.remove(&5);
    row.remove(&6);
    let Some(score) = score else {
        return row;
    };
    let text = match row.get(&2) {
        Some(Value::Bytes(text)) => String::from_utf8_lossy(text).into_owned(),
        _ => String::new(),
    };
    row.insert(4, Value::Float64(score));
    if let Some(query) = query {
        row.insert(
            5,
            Value::Bytes(fts_snippet(&text, query, options).into_bytes()),
        );
        row.insert(
            6,
            Value::Bytes(fts_highlight(&text, query, options).into_bytes()),
        );
    }
    row
}

#[derive(Debug, Clone)]
struct TokenSpan {
    term: String,
    start: usize,
    end: usize,
}

fn token_spans_with_options(input: &str, options: &FtsOptions) -> Vec<TokenSpan> {
    let mut spans = Vec::new();
    let mut start = None;
    for (idx, ch) in input.char_indices() {
        if ch.is_ascii_alphanumeric() {
            start.get_or_insert(idx);
        } else if let Some(lo) = start.take() {
            push_token_span(input, &mut spans, lo, idx, options);
        }
    }
    if let Some(lo) = start {
        push_token_span(input, &mut spans, lo, input.len(), options);
    }
    spans
}

fn push_token_span(
    input: &str,
    spans: &mut Vec<TokenSpan>,
    start: usize,
    end: usize,
    options: &FtsOptions,
) {
    if start < end {
        let term = options.normalize(&input[start..end]);
        if options.keep_term(&term) {
            spans.push(TokenSpan { term, start, end });
        }
    }
}

fn fts_snippet(text: &str, query: &FtsQuery, options: &FtsOptions) -> String {
    let spans = token_spans_with_options(text, options);
    if spans.is_empty() {
        return String::new();
    }
    let first_match = spans
        .iter()
        .position(|span| token_matches_positive_clause(&span.term, query))
        .unwrap_or(0);
    let lo = first_match.saturating_sub(3);
    let hi = (first_match + 5).min(spans.len());
    let mut out = Vec::new();
    if lo > 0 {
        out.push("...".to_string());
    }
    for span in &spans[lo..hi] {
        let token = &text[span.start..span.end];
        if token_matches_positive_clause(&span.term, query) {
            out.push(format!("[{token}]"));
        } else {
            out.push(token.to_string());
        }
    }
    if hi < spans.len() {
        out.push("...".to_string());
    }
    out.join(" ")
}

fn fts_highlight(text: &str, query: &FtsQuery, options: &FtsOptions) -> String {
    let spans = token_spans_with_options(text, options);
    if spans.is_empty() {
        return text.to_string();
    }
    let mut out = String::new();
    let mut cursor = 0;
    for span in spans {
        out.push_str(&text[cursor..span.start]);
        let token = &text[span.start..span.end];
        if token_matches_positive_clause(&span.term, query) {
            out.push_str("<b>");
            out.push_str(token);
            out.push_str("</b>");
        } else {
            out.push_str(token);
        }
        cursor = span.end;
    }
    out.push_str(&text[cursor..]);
    out
}

struct RTreeRectsModule;

impl ExternalTableModule for RTreeRectsModule {
    fn name(&self) -> &str {
        "rtree_rects"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: rtree_rects_schema(),
            hidden_columns: vec![
                "query_min_x".to_string(),
                "query_max_x".to_string(),
                "query_min_y".to_string(),
                "query_max_y".to_string(),
            ],
            capabilities: ModuleCapabilities {
                writable: true,
                deterministic: true,
                trigger_safe: true,
                ..ModuleCapabilities::default()
            },
        }
    }

    fn indexes_with_control(
        &self,
        context: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Vec<ExternalModuleIndex>> {
        context.control.checkpoint()?;
        Ok(vec![ExternalModuleIndex::new(
            format!("{}_rtree_spatial", entry.name),
            vec![2, 3, 4, 5],
        )])
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        let rows = read_state_rows(ctx.database, entry)?;
        let index = RTreeSpatialIndex::build(&rows);
        Ok(Arc::new(RTreeRectsExternalTable {
            schema: arrow_conv::arrow_schema(&entry.declared_schema)?,
            core_schema: entry.declared_schema.clone(),
            index,
            rows,
        }))
    }

    fn read_rows_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Vec<HashMap<u16, Value>>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        read_state_rows(ctx.database, entry)
    }

    fn prepare_rows_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
        rows: Vec<HashMap<u16, Value>>,
    ) -> Result<Vec<u8>> {
        ctx.control.checkpoint()?;
        ensure_no_args(entry, self.name())?;
        let rows = rows
            .into_iter()
            .map(|mut row| {
                for column_id in 6..=9 {
                    row.remove(&column_id);
                }
                row
            })
            .collect::<Vec<_>>();
        validate_external_rows(&entry.declared_schema, &rows)?;
        encode_state_rows(&rows)
    }
}

#[derive(Clone)]
struct RTreeRectsExternalTable {
    schema: SchemaRef,
    core_schema: CoreSchema,
    index: RTreeSpatialIndex,
    rows: Vec<HashMap<u16, Value>>,
}

impl std::fmt::Debug for RTreeRectsExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RTreeRectsExternalTable")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for RTreeRectsExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        Ok(ExternalPlan::new(
            request
                .raw_filters
                .iter()
                .map(|filter| {
                    if rtree_query_bound(filter).is_some()
                        || rtree_query_rect_function(filter).is_some()
                    {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                })
                .collect(),
            None,
            1.0,
            false,
        ))
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        external_checkpoint(Some(control), 0)?;
        let mut hidden_bounds = QueryRect::default();
        let mut has_hidden_bounds = false;
        for (column, value) in request
            .raw_filters
            .iter()
            .filter_map(|filter| rtree_query_bound(filter))
        {
            hidden_bounds.set(column, value);
            has_hidden_bounds = true;
        }
        let mut query_rects = request
            .raw_filters
            .iter()
            .filter_map(|filter| rtree_query_rect_function(filter))
            .collect::<Vec<_>>();
        if has_hidden_bounds || query_rects.is_empty() {
            query_rects.push(hidden_bounds);
        }
        let mut candidate_rows: Option<BTreeSet<usize>> = None;
        for bounds in &query_rects {
            let mut next = BTreeSet::new();
            for (index, row) in self.index.candidates(*bounds).into_iter().enumerate() {
                external_checkpoint(Some(control), index)?;
                next.insert(row);
            }
            candidate_rows = Some(match candidate_rows.take() {
                Some(current) => {
                    let mut intersection = BTreeSet::new();
                    for (index, row) in current.into_iter().enumerate() {
                        external_checkpoint(Some(control), index)?;
                        if next.contains(&row) {
                            intersection.insert(row);
                        }
                    }
                    intersection
                }
                None => next,
            });
        }
        let candidate_rows =
            candidate_rows.unwrap_or_else(|| self.index.all_rows.iter().copied().collect());
        let mut rows = Vec::new();
        for (index, row) in self.rows.iter().enumerate() {
            external_checkpoint(Some(control), index)?;
            if candidate_rows.contains(&index)
                && query_rects
                    .iter()
                    .all(|bounds| rtree_row_matches(row, *bounds))
            {
                rows.push(row.clone());
            }
        }
        external_checkpoint(Some(control), 0)?;
        project_scan_with_control(
            self.schema.clone(),
            core_rows_to_batches(&self.core_schema, rows)
                .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

#[derive(Clone, Copy)]
struct QueryRect {
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
}

impl Default for QueryRect {
    fn default() -> Self {
        Self {
            min_x: f64::NEG_INFINITY,
            max_x: f64::INFINITY,
            min_y: f64::NEG_INFINITY,
            max_y: f64::INFINITY,
        }
    }
}

impl QueryRect {
    fn set(&mut self, column: &str, value: f64) {
        match column {
            "query_min_x" => self.min_x = value,
            "query_max_x" => self.max_x = value,
            "query_min_y" => self.min_y = value,
            "query_max_y" => self.max_y = value,
            _ => {}
        }
    }
}

#[derive(Clone)]
struct RTreeSpatialIndex {
    all_rows: Vec<usize>,
    min_x: Vec<(f64, usize)>,
    max_x: Vec<(f64, usize)>,
    min_y: Vec<(f64, usize)>,
    max_y: Vec<(f64, usize)>,
}

impl RTreeSpatialIndex {
    fn build(rows: &[HashMap<u16, Value>]) -> Self {
        let mut all_rows = Vec::new();
        let mut min_x = Vec::new();
        let mut max_x = Vec::new();
        let mut min_y = Vec::new();
        let mut max_y = Vec::new();
        for (idx, row) in rows.iter().enumerate() {
            let Some(x0) = row_f64(row, 2) else {
                continue;
            };
            let Some(x1) = row_f64(row, 3) else {
                continue;
            };
            let Some(y0) = row_f64(row, 4) else {
                continue;
            };
            let Some(y1) = row_f64(row, 5) else {
                continue;
            };
            if !(x0.is_finite() && x1.is_finite() && y0.is_finite() && y1.is_finite()) {
                continue;
            }
            all_rows.push(idx);
            min_x.push((x0, idx));
            max_x.push((x1, idx));
            min_y.push((y0, idx));
            max_y.push((y1, idx));
        }
        for values in [&mut min_x, &mut max_x, &mut min_y, &mut max_y] {
            values.sort_by(|left, right| left.0.total_cmp(&right.0));
        }
        Self {
            all_rows,
            min_x,
            max_x,
            min_y,
            max_y,
        }
    }

    fn candidates(&self, query: QueryRect) -> Vec<usize> {
        let mut rows = self.lte(&self.min_x, query.max_x);
        for next in [
            self.gte(&self.max_x, query.min_x),
            self.lte(&self.min_y, query.max_y),
            self.gte(&self.max_y, query.min_y),
        ] {
            rows = rows.intersection(&next).copied().collect();
            if rows.is_empty() {
                break;
            }
        }
        rows.into_iter().collect()
    }

    fn lte(&self, values: &[(f64, usize)], bound: f64) -> BTreeSet<usize> {
        if bound == f64::INFINITY {
            return self.all_rows.iter().copied().collect();
        }
        if bound.is_nan() {
            return BTreeSet::new();
        }
        let end = values.partition_point(|(value, _)| *value <= bound);
        values[..end].iter().map(|(_, idx)| *idx).collect()
    }

    fn gte(&self, values: &[(f64, usize)], bound: f64) -> BTreeSet<usize> {
        if bound == f64::NEG_INFINITY {
            return self.all_rows.iter().copied().collect();
        }
        if bound.is_nan() {
            return BTreeSet::new();
        }
        let start = values.partition_point(|(value, _)| *value < bound);
        values[start..].iter().map(|(_, idx)| *idx).collect()
    }
}

fn rtree_query_rect_function(expr: &Expr) -> Option<QueryRect> {
    let Expr::ScalarFunction(sf) = expr else {
        return None;
    };
    if !sf.func.name().eq_ignore_ascii_case("rtree_intersects") || sf.args.len() != 8 {
        return None;
    }
    for (arg, expected) in sf
        .args
        .iter()
        .take(4)
        .zip(["min_x", "max_x", "min_y", "max_y"])
    {
        let Expr::Column(column) = arg else {
            return None;
        };
        if column.name != expected {
            return None;
        }
    }
    Some(QueryRect {
        min_x: literal_f64_from_expr(&sf.args[4])?,
        max_x: literal_f64_from_expr(&sf.args[5])?,
        min_y: literal_f64_from_expr(&sf.args[6])?,
        max_y: literal_f64_from_expr(&sf.args[7])?,
    })
}

fn literal_f64_from_expr(expr: &Expr) -> Option<f64> {
    match expr {
        Expr::Literal(literal, _) => literal_f64(literal),
        _ => None,
    }
}

fn rtree_rects_schema() -> CoreSchema {
    CoreSchema {
        schema_id: 0,
        columns: vec![
            CoreColumnDef {
                id: 1,
                name: "id".to_string(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            rect_column(2, "min_x"),
            rect_column(3, "max_x"),
            rect_column(4, "min_y"),
            rect_column(5, "max_y"),
            rect_column(6, "query_min_x"),
            rect_column(7, "query_max_x"),
            rect_column(8, "query_min_y"),
            rect_column(9, "query_max_y"),
        ],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn rect_column(id: u16, name: &str) -> CoreColumnDef {
    CoreColumnDef {
        id,
        name: name.to_string(),
        ty: TypeId::Float64,
        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
        default_value: None,
        embedding_source: None,
    }
}

fn rtree_query_bound(expr: &Expr) -> Option<(&str, f64)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::Eq => {
            match (left.as_ref(), right.as_ref()) {
                (Expr::Column(column), Expr::Literal(literal, _)) => {
                    hidden_rect_column(&column.name).zip(literal_f64(literal))
                }
                (Expr::Literal(literal, _), Expr::Column(column)) => {
                    hidden_rect_column(&column.name).zip(literal_f64(literal))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

fn hidden_rect_column(name: &str) -> Option<&str> {
    match name {
        "query_min_x" | "query_max_x" | "query_min_y" | "query_max_y" => Some(name),
        _ => None,
    }
}

fn rtree_row_matches(row: &HashMap<u16, Value>, query: QueryRect) -> bool {
    let Some(min_x) = row_f64(row, 2) else {
        return false;
    };
    let Some(max_x) = row_f64(row, 3) else {
        return false;
    };
    let Some(min_y) = row_f64(row, 4) else {
        return false;
    };
    let Some(max_y) = row_f64(row, 5) else {
        return false;
    };
    max_x >= query.min_x && min_x <= query.max_x && max_y >= query.min_y && min_y <= query.max_y
}

fn row_f64(row: &HashMap<u16, Value>, column_id: u16) -> Option<f64> {
    match row.get(&column_id) {
        Some(Value::Float64(value)) => Some(*value),
        Some(Value::Int64(value)) => Some(*value as f64),
        _ => None,
    }
}

struct SeriesModule;

impl ExternalTableModule for SeriesModule {
    fn name(&self) -> &str {
        "series"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: CoreSchema {
                schema_id: 0,
                columns: vec![
                    CoreColumnDef {
                        id: 1,
                        name: "value".to_string(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty(),
                        default_value: None,
                        embedding_source: None,
                    },
                    CoreColumnDef {
                        id: 2,
                        name: "start".to_string(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                    CoreColumnDef {
                        id: 3,
                        name: "stop".to_string(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                    CoreColumnDef {
                        id: 4,
                        name: "step".to_string(),
                        ty: TypeId::Int64,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                indexes: Vec::new(),
                colocation: Vec::new(),
                constraints: Default::default(),
                clustered: false,
            },
            hidden_columns: vec!["start".to_string(), "stop".to_string(), "step".to_string()],
            capabilities: ModuleCapabilities {
                read_only: true,
                deterministic: true,
                trigger_safe: true,
                ..ModuleCapabilities::default()
            },
        }
    }

    fn connect_with_control(
        &self,
        ctx: &ExternalExecutionContext<'_>,
        entry: &ExternalTableEntry,
    ) -> Result<Arc<dyn ExternalTable>> {
        ctx.control.checkpoint()?;
        let (start, stop, step) = series_args(entry)?;
        let table = SeriesExternalTable::new(start, stop, step)?;
        Ok(Arc::new(table))
    }
}

struct SeriesExternalTable {
    start: i64,
    stop: i64,
    step: i64,
    schema: SchemaRef,
}

impl SeriesExternalTable {
    fn new(start: i64, stop: i64, step: i64) -> Result<Self> {
        if step == 0 {
            return Err(MongrelQueryError::Schema(
                "series step must not be 0".into(),
            ));
        }
        Ok(Self {
            start,
            stop,
            step,
            schema: Arc::new(ArrowSchema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
        })
    }

    fn batches(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<Vec<RecordBatch>> {
        let mut values = Vec::new();
        let mut current = self.start;
        let mut scanned = 0;
        while if self.step > 0 {
            current <= self.stop
        } else {
            current >= self.stop
        } {
            external_checkpoint(Some(control), scanned)?;
            scanned += 1;
            if values.len() >= 1_000_000 {
                return Err(DataFusionError::Plan(
                    "series output is capped at 1,000,000 rows".into(),
                ));
            }
            if request
                .filters
                .iter()
                .all(|filter| series_filter_matches(filter, current).unwrap_or(true))
            {
                values.push(current);
                if request.limit.is_some_and(|limit| values.len() >= limit) {
                    break;
                }
            }
            current = current.saturating_add(self.step);
            if (self.step > 0 && current == i64::MAX) || (self.step < 0 && current == i64::MIN) {
                break;
            }
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Int64Array::from(values)) as ArrayRef],
        )?;
        Ok(vec![batch])
    }
}

impl std::fmt::Debug for SeriesExternalTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeriesExternalTable")
            .field("start", &self.start)
            .field("stop", &self.stop)
            .field("step", &self.step)
            .finish_non_exhaustive()
    }
}

impl ExternalTable for SeriesExternalTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn plan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &ExecutionControl,
    ) -> DFResult<ExternalPlan> {
        external_df_checkpoint(control)?;
        Ok(ExternalPlan::new(
            request
                .filters
                .iter()
                .map(|filter| {
                    if series_filter_supported(filter) {
                        TableProviderFilterPushDown::Exact
                    } else {
                        TableProviderFilterPushDown::Unsupported
                    }
                })
                .collect(),
            None,
            1.0,
            self.step > 0,
        ))
    }

    fn scan_with_control(
        &self,
        request: &ExternalPlanRequest<'_>,
        control: &mongreldb_core::ExecutionControl,
    ) -> DFResult<ExternalScan> {
        external_checkpoint(Some(control), 0)?;
        let batches = self.batches(request, control)?;
        external_checkpoint(Some(control), 0)?;
        project_scan_with_control(
            self.schema.clone(),
            batches,
            request.projection.as_deref(),
            request.limit,
            Some(control),
        )
    }
}

fn series_filter_supported(filter: &ExternalFilter) -> bool {
    match filter {
        ExternalFilter::And(filters) => filters.iter().all(series_filter_supported),
        ExternalFilter::Compare {
            column_index,
            value,
            ..
        } => *column_index == 0 && literal_i64(value).is_some(),
        ExternalFilter::Unsupported { .. } => false,
    }
}

fn series_filter_matches(filter: &ExternalFilter, value: i64) -> Option<bool> {
    match filter {
        ExternalFilter::And(filters) => filters.iter().try_fold(true, |matches, filter| {
            Some(matches && series_filter_matches(filter, value)?)
        }),
        ExternalFilter::Compare {
            column_index,
            op,
            value: literal,
        } if *column_index == 0 => {
            let literal = literal_i64(literal)?;
            Some(match op {
                ExternalFilterOp::Eq => value == literal,
                ExternalFilterOp::NotEq => value != literal,
                ExternalFilterOp::Gt => value > literal,
                ExternalFilterOp::GtEq => value >= literal,
                ExternalFilterOp::Lt => value < literal,
                ExternalFilterOp::LtEq => value <= literal,
            })
        }
        _ => None,
    }
}

fn literal_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        _ => None,
    }
}

fn literal_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        _ => literal_i64(value).map(|value| value as f64),
    }
}

fn literal_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Some(v.clone()),
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::LargeBinary(Some(v)) => String::from_utf8(v.clone()).ok(),
        _ => None,
    }
}

fn series_args(entry: &ExternalTableEntry) -> Result<(i64, i64, i64)> {
    let values = entry
        .args
        .iter()
        .map(|arg| match arg {
            ModuleArg::Ident(value) | ModuleArg::String(value) | ModuleArg::Number(value) => {
                value.parse::<i64>().map_err(|e| {
                    MongrelQueryError::Schema(format!(
                        "series module argument {value:?} must be an integer: {e}"
                    ))
                })
            }
        })
        .collect::<Result<Vec<_>>>()?;
    match values.as_slice() {
        [] => Ok((0, -1, 1)),
        [stop] => Ok((0, *stop, 1)),
        [start, stop] => Ok((*start, *stop, 1)),
        [start, stop, step] => Ok((*start, *stop, *step)),
        _ => Err(MongrelQueryError::Schema(
            "series external table accepts at most three arguments".into(),
        )),
    }
}
