//! P4.1 — multi-table SQL over a Database.

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::TableProviderFilterPushDown;
use mongreldb_core::{
    database::VTAB_DIR, schema::*, Database, ModuleCapabilities, StoredTrigger, TriggerCell,
    TriggerDefinition, TriggerEvent, TriggerProgram, TriggerStep, TriggerTarget, TriggerTiming,
    TriggerValue, Value,
};
use mongreldb_query::{
    ExternalModuleDescriptor, ExternalModuleIndex, ExternalPlan, ExternalPlanRequest, ExternalScan,
    ExternalTable, ExternalTableModule, ExternalTxn, ExternalWriteOp, ExternalWriteResult,
    ModuleConnectCtx, MongrelQueryError, MongrelSession,
};
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tempfile::tempdir;

struct AppRowsModule {
    destroyed: Arc<AtomicBool>,
    plan_calls: Arc<AtomicUsize>,
}

impl AppRowsModule {
    fn new(destroyed: Arc<AtomicBool>) -> Self {
        Self {
            destroyed,
            plan_calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn with_plan_counter(destroyed: Arc<AtomicBool>, plan_calls: Arc<AtomicUsize>) -> Self {
        Self {
            destroyed,
            plan_calls,
        }
    }
}

impl ExternalTableModule for AppRowsModule {
    fn name(&self) -> &str {
        "app_rows"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: app_rows_schema(),
            hidden_columns: Vec::new(),
            capabilities: ModuleCapabilities {
                read_only: true,
                deterministic: true,
                trigger_safe: true,
                ..ModuleCapabilities::default()
            },
        }
    }

    fn indexes(
        &self,
        entry: &mongreldb_core::ExternalTableEntry,
    ) -> mongreldb_query::Result<Vec<ExternalModuleIndex>> {
        Ok(vec![ExternalModuleIndex::new(
            format!("{}_label_lookup", entry.name),
            vec![2],
        )])
    }

    fn connect(
        &self,
        _ctx: &ModuleConnectCtx<'_>,
        _entry: &mongreldb_core::ExternalTableEntry,
    ) -> mongreldb_query::Result<Arc<dyn ExternalTable>> {
        Ok(Arc::new(AppRowsTable {
            schema: Arc::new(ArrowSchema::new(vec![
                Field::new("id", ArrowDataType::Int64, false),
                Field::new("label", ArrowDataType::Utf8, false),
            ])),
            plan_calls: Arc::clone(&self.plan_calls),
        }))
    }

    fn destroy(
        &self,
        _ctx: &ModuleConnectCtx<'_>,
        _entry: &mongreldb_core::ExternalTableEntry,
    ) -> mongreldb_query::Result<()> {
        self.destroyed.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Debug)]
struct AppRowsTable {
    schema: Arc<ArrowSchema>,
    plan_calls: Arc<AtomicUsize>,
}

impl ExternalTable for AppRowsTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn plan(&self, request: &ExternalPlanRequest<'_>) -> DFResult<ExternalPlan> {
        self.plan_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ExternalPlan::new(
            request
                .filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect(),
            Some(2),
            1.0,
            false,
        ))
    }

    fn scan(&self, request: &ExternalPlanRequest<'_>) -> DFResult<ExternalScan> {
        let full_columns = vec![
            Arc::new(Int64Array::from(vec![7, 8])) as ArrayRef,
            Arc::new(StringArray::from(vec!["seven", "eight"])) as ArrayRef,
        ];
        let (schema, columns) = if let Some(projection) = request.projection.as_deref() {
            let fields = projection
                .iter()
                .map(|idx| self.schema.field(*idx).clone())
                .collect::<Vec<_>>();
            let columns = projection
                .iter()
                .map(|idx| full_columns[*idx].clone())
                .collect::<Vec<_>>();
            (Arc::new(ArrowSchema::new(fields)), columns)
        } else {
            (self.schema.clone(), full_columns)
        };
        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        Ok(ExternalScan {
            schema,
            batches: vec![batch],
        })
    }
}

fn app_rows_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "label".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

struct AppTxnModule;

impl ExternalTableModule for AppTxnModule {
    fn name(&self) -> &str {
        "app_txn"
    }

    fn descriptor(&self) -> ExternalModuleDescriptor {
        ExternalModuleDescriptor {
            schema: app_txn_schema(),
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

    fn connect(
        &self,
        ctx: &ModuleConnectCtx<'_>,
        entry: &mongreldb_core::ExternalTableEntry,
    ) -> mongreldb_query::Result<Arc<dyn ExternalTable>> {
        Ok(Arc::new(AppTxnTable {
            schema: Arc::new(ArrowSchema::new(vec![
                Field::new("key", ArrowDataType::Utf8, false),
                Field::new("value", ArrowDataType::Utf8, true),
            ])),
            rows: app_txn_rows_from_value(ctx.read_state(entry, b"rows")?.as_deref())?,
        }))
    }

    fn read_rows(
        &self,
        ctx: &ModuleConnectCtx<'_>,
        entry: &mongreldb_core::ExternalTableEntry,
    ) -> mongreldb_query::Result<Vec<std::collections::HashMap<u16, Value>>> {
        app_txn_rows_from_value(ctx.read_state(entry, b"rows")?.as_deref())
    }

    fn rows_from_state(
        &self,
        state: &[u8],
    ) -> mongreldb_query::Result<Vec<std::collections::HashMap<u16, Value>>> {
        let txn = ExternalTxn::new(state.to_vec());
        app_txn_rows_from_value(txn.read_state(b"rows")?.as_deref())
    }

    fn write(
        &self,
        _ctx: &ModuleConnectCtx<'_>,
        _entry: &mongreldb_core::ExternalTableEntry,
        op: ExternalWriteOp,
        txn: &mut ExternalTxn,
    ) -> mongreldb_query::Result<ExternalWriteResult> {
        let (rows, changes) = match op {
            ExternalWriteOp::Insert { rows: inserted } => {
                let mut rows = app_txn_rows_from_value(txn.read_state(b"rows")?.as_deref())?;
                let changes = inserted.len() as u64;
                rows.extend(inserted);
                (rows, changes)
            }
            ExternalWriteOp::ReplaceRows { rows, changes } => (rows, changes),
        };
        txn.put_state(b"rows", &app_txn_rows_to_value(&rows)?)?;
        Ok(ExternalWriteResult::new(changes))
    }
}

#[derive(Debug)]
struct AppTxnTable {
    schema: Arc<ArrowSchema>,
    rows: Vec<std::collections::HashMap<u16, Value>>,
}

impl ExternalTable for AppTxnTable {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn plan(&self, request: &ExternalPlanRequest<'_>) -> DFResult<ExternalPlan> {
        Ok(ExternalPlan::new(
            request
                .filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect(),
            Some(self.rows.len() as u64),
            self.rows.len() as f64,
            false,
        ))
    }

    fn scan(&self, request: &ExternalPlanRequest<'_>) -> DFResult<ExternalScan> {
        let mut rows = self.rows.clone();
        rows.sort_by_key(app_txn_key);
        let keys = rows.iter().map(app_txn_key).collect::<Vec<_>>();
        let values = rows.iter().map(app_txn_value).collect::<Vec<_>>();
        let batches = vec![RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)) as ArrayRef,
                Arc::new(StringArray::from(values)) as ArrayRef,
            ],
        )
        .map_err(|e| DataFusionError::Execution(e.to_string()))?];
        project_app_scan(self.schema.clone(), batches, request.projection.as_deref())
    }
}

#[derive(Serialize, Deserialize)]
struct AppTxnStoredRow {
    cells: Vec<(u16, Value)>,
}

fn app_txn_schema() -> Schema {
    Schema {
        schema_id: 0,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "key".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "value".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn app_txn_rows_from_value(
    value: Option<&[u8]>,
) -> mongreldb_query::Result<Vec<std::collections::HashMap<u16, Value>>> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let stored: Vec<AppTxnStoredRow> = serde_json::from_slice(value)
        .map_err(|e| mongreldb_query::MongrelQueryError::Schema(e.to_string()))?;
    Ok(stored
        .into_iter()
        .map(|row| row.cells.into_iter().collect())
        .collect())
}

fn app_txn_rows_to_value(
    rows: &[std::collections::HashMap<u16, Value>],
) -> mongreldb_query::Result<Vec<u8>> {
    let mut keys = std::collections::HashSet::new();
    let stored = rows
        .iter()
        .map(|row| {
            let key = app_txn_key(row);
            if !keys.insert(key.clone()) {
                return Err(mongreldb_query::MongrelQueryError::Schema(format!(
                    "duplicate app_txn key {key:?}"
                )));
            }
            let mut cells = row
                .iter()
                .map(|(id, value)| (*id, value.clone()))
                .collect::<Vec<_>>();
            cells.sort_by_key(|(id, _)| *id);
            Ok(AppTxnStoredRow { cells })
        })
        .collect::<mongreldb_query::Result<Vec<_>>>()?;
    serde_json::to_vec(&stored)
        .map_err(|e| mongreldb_query::MongrelQueryError::Schema(e.to_string()))
}

fn app_txn_key(row: &std::collections::HashMap<u16, Value>) -> String {
    match row.get(&1) {
        Some(Value::Bytes(value)) => String::from_utf8_lossy(value).into_owned(),
        _ => String::new(),
    }
}

fn app_txn_value(row: &std::collections::HashMap<u16, Value>) -> Option<String> {
    match row.get(&2) {
        Some(Value::Bytes(value)) => Some(String::from_utf8_lossy(value).into_owned()),
        Some(Value::Null) | None => None,
        _ => Some(String::new()),
    }
}

fn project_app_scan(
    full_schema: Arc<ArrowSchema>,
    batches: Vec<RecordBatch>,
    projection: Option<&[usize]>,
) -> DFResult<ExternalScan> {
    let Some(projection) = projection else {
        return Ok(ExternalScan {
            schema: full_schema,
            batches,
        });
    };
    let schema = Arc::new(ArrowSchema::new(
        projection
            .iter()
            .map(|idx| full_schema.field(*idx).clone())
            .collect::<Vec<_>>(),
    ));
    let batches = batches
        .into_iter()
        .map(|batch| {
            let columns = projection
                .iter()
                .map(|idx| batch.column(*idx).clone())
                .collect::<Vec<_>>();
            RecordBatch::try_new(schema.clone(), columns)
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(ExternalScan { schema, batches })
}

fn orders_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "customer_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn customers_schema() -> Schema {
    Schema {
        schema_id: 2,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "name".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn i64_values(batches: &[RecordBatch], column: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn string_values(batches: &[RecordBatch], column: usize) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn bool_values(batches: &[RecordBatch], column: usize) -> Vec<bool> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn f64_values(batches: &[RecordBatch], column: usize) -> Vec<f64> {
    batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            (0..array.len())
                .map(|row| array.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn cross_table_join_over_database() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    db.create_table("orders", orders_schema()).unwrap();
    db.create_table("customers", customers_schema()).unwrap();

    db.transaction(|t| {
        t.put(
            "customers",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"Alice".to_vec()))],
        )?;
        t.put(
            "customers",
            vec![(1, Value::Int64(2)), (2, Value::Bytes(b"Bob".to_vec()))],
        )?;
        t.put("orders", vec![(1, Value::Int64(100)), (2, Value::Int64(1))])?;
        t.put("orders", vec![(1, Value::Int64(101)), (2, Value::Int64(2))])?;
        t.put("orders", vec![(1, Value::Int64(102)), (2, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // Simple queries work.
    let batches = session.run("SELECT * FROM orders").await.unwrap();
    assert_eq!(total_rows(&batches), 3);

    let batches = session.run("SELECT * FROM customers").await.unwrap();
    assert_eq!(total_rows(&batches), 2);

    // Cross-table join.
    let batches = session
        .run("SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id")
        .await
        .unwrap();
    assert_eq!(total_rows(&batches), 3);

    // COUNT(*) is O(1).
    let batches = session.run("SELECT COUNT(*) FROM orders").await.unwrap();
    assert_eq!(total_rows(&batches), 1);
}

/// Priority 13 + 8: the query trace reports which join path ran and how long
/// logical planning took.
#[tokio::test]
async fn join_and_planning_diagnostics() {
    use mongreldb_core::trace::JoinMode;
    // orders carries a bitmap index on the FK join column, which enables the
    // native broadcast FK-bitmap join even without a WHERE on the PK side.
    let orders = Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
            },
            ColumnDef {
                id: 2,
                name: "customer_id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
            },
        ],
        indexes: vec![IndexDef {
            name: "orders_cust_bm".into(),
            column_id: 2,
            kind: IndexKind::Bitmap,
            predicate: None,
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("orders", orders).unwrap();
    db.create_table("customers", customers_schema()).unwrap();
    db.transaction(|t| {
        t.put(
            "customers",
            vec![(1, Value::Int64(1)), (2, Value::Bytes(b"Alice".to_vec()))],
        )?;
        t.put(
            "customers",
            vec![(1, Value::Int64(2)), (2, Value::Bytes(b"Bob".to_vec()))],
        )?;
        t.put("orders", vec![(1, Value::Int64(100)), (2, Value::Int64(1))])?;
        t.put("orders", vec![(1, Value::Int64(101)), (2, Value::Int64(2))])?;
        t.put("orders", vec![(1, Value::Int64(102)), (2, Value::Int64(1))])?;
        Ok(())
    })
    .unwrap();
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // Non-join query ⇒ JoinMode::None; a cold query records planning time.
    let (_b, t) = session
        .run_sql_traced("SELECT * FROM orders")
        .await
        .unwrap();
    assert_eq!(t.join_mode, JoinMode::None);
    assert!(
        t.planning_nanos > 0,
        "cold query should record planning time"
    );

    // PK↔FK equi-join over the bitmap-indexed FK ⇒ native FK-bitmap path.
    let (b, t) = session
        .run_sql_traced(
            "SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
        )
        .await
        .unwrap();
    assert_eq!(total_rows(&b), 3);
    assert_eq!(t.join_mode, JoinMode::FkBitmap);
}

#[tokio::test]
async fn database_session_cache_invalidates_on_commit() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.create_table("t", orders_schema()).unwrap();

    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        Ok(())
    })
    .unwrap();

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // First query populates the cache.
    let batches = session.run("SELECT COUNT(*) FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 1);

    // Commit new data — cache must invalidate (epoch changes).
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])?;
        Ok(())
    })
    .unwrap();

    // Re-run — new result.
    let batches = session.run("SELECT * FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn create_and_drop_table_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // CREATE TABLE via SQL.
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    // Insert via the Database (SQL insert is not yet wired; use the native API).
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(42))])?;
        Ok(())
    })
    .unwrap();

    // SELECT works.
    let batches = session.run("SELECT * FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 1);

    // DROP TABLE via SQL.
    session.run("DROP TABLE t").await.unwrap();

    // Table is gone — querying it should fail.
    let result = session.run("SELECT * FROM t").await;
    assert!(result.is_err(), "expected error after DROP TABLE, got Ok");
}

#[tokio::test]
async fn ddl_is_case_insensitive() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // Mixed-case keywords.
    session
        .run("Create Table t (id BIGINT Primary Key, v BIGINT)")
        .await
        .unwrap();
    assert_eq!(db.table_names(), vec!["t".to_string()]);

    session.run("Drop Table t").await.unwrap();
    assert!(db.table_names().is_empty());
}

#[tokio::test]
async fn ddl_with_if_not_exists_and_if_exists() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    // CREATE TABLE IF NOT EXISTS
    session
        .run("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    assert_eq!(db.table_names(), vec!["t".to_string()]);

    // DROP TABLE IF EXISTS on a live table succeeds.
    session.run("DROP TABLE IF EXISTS t").await.unwrap();
    assert!(db.table_names().is_empty());

    // DROP TABLE IF EXISTS on a non-existent table succeeds (no error).
    session.run("DROP TABLE IF EXISTS nonexist").await.unwrap();
}

#[tokio::test]
async fn schema_id_is_unique_per_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());

    let _ = db.create_table("a", orders_schema()).unwrap();
    let _ = db.create_table("b", customers_schema()).unwrap();

    let schema_a = db.table("a").unwrap().lock().schema().clone();
    let schema_b = db.table("b").unwrap().lock().schema().clone();
    assert_ne!(
        schema_a.schema_id, schema_b.schema_id,
        "schema_ids must be unique across tables"
    );
}

// --- AUTOINCREMENT via SQL DDL ------------------------------------------------

#[tokio::test]
async fn create_table_autoincrement_sets_flag_and_assigns_ids() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE counters (id BIGINT PRIMARY KEY AUTOINCREMENT, label TEXT)")
        .await
        .unwrap();

    // The AUTO_INCREMENT flag reached the engine schema from the SQL parser.
    let table = db.table("counters").unwrap();
    {
        let guard = table.lock();
        let id_col = guard
            .schema()
            .column("id")
            .expect("id column exists")
            .clone();
        assert!(id_col.flags.contains(ColumnFlags::AUTO_INCREMENT));
        assert!(id_col.flags.contains(ColumnFlags::PRIMARY_KEY));
    }

    // Omitting the PK triggers engine allocation (1-based, monotonic). The
    // returned assigned id is the direct proof; reading via the SQL session is
    // avoided because a single-table put does not advance the database-visible
    // epoch the session keys off.
    let assigned1 = table
        .lock()
        .put_returning(vec![(2, Value::Bytes(b"a".to_vec()))])
        .unwrap()
        .1;
    let assigned2 = table
        .lock()
        .put_returning(vec![(2, Value::Bytes(b"b".to_vec()))])
        .unwrap()
        .1;
    assert_eq!(assigned1, Some(1));
    assert_eq!(assigned2, Some(2));
}

#[tokio::test]
async fn autoincrement_keyword_is_case_insensitive() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("Create Table t (id Bigint Primary Key Autoincrement, v Bigint)")
        .await
        .unwrap();
    let flags = db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("id")
        .unwrap()
        .flags;
    assert!(flags.contains(ColumnFlags::AUTO_INCREMENT));
}

#[tokio::test]
async fn auto_increment_underscore_spelling_is_accepted() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY AUTO_INCREMENT, v BIGINT)")
        .await
        .unwrap();
    let flags = db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("id")
        .unwrap()
        .flags;
    assert!(flags.contains(ColumnFlags::AUTO_INCREMENT));
}

#[tokio::test]
async fn autoincrement_on_non_primary_key_is_rejected_with_no_dangling_wal_entry() {
    let dir = tempdir().unwrap();
    {
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(Arc::clone(&db)).unwrap();

        // AUTO_INCREMENT on a non-PK column violates the engine contract; the
        // schema must be rejected at creation.
        let result = session
            .run("CREATE TABLE bad (id BIGINT PRIMARY KEY, seq BIGINT AUTO_INCREMENT)")
            .await;
        assert!(
            result.is_err(),
            "AUTO_INCREMENT on a non-PK column must be rejected"
        );
        // In-process, the table was never published to the catalog.
        assert!(db.table_names().is_empty());

        // Drop the live handles; only the on-disk WAL remains for reopen.
        drop(session);
        drop(db);
    }

    // A rejected schema must leave NO durable trace. If the DDL had been
    // appended to the shared WAL before validation, `recover_ddl_from_wal`
    // would replay it (without re-validating) and resurrect "bad" in the
    // catalog — so an empty catalog after reopen proves the validation ran
    // before the WAL mutation.
    let reopened = Database::open(dir.path()).unwrap();
    assert!(
        reopened.table_names().is_empty(),
        "a rejected CREATE TABLE must not leave a table in the catalog after reopen"
    );
}

// --- ALTER TABLE ... RENAME TO ... -------------------------------------------

#[tokio::test]
async fn alter_table_rename_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    // Insert via the Database so the row is committed and visible to the
    // session's epoch before the rename.
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        t.put("t", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])
    })
    .unwrap();

    session.run("ALTER TABLE t RENAME TO u").await.unwrap();

    // The old name is gone from the catalog and from DataFusion.
    assert!(!db.table_names().contains(&"t".to_string()));
    assert!(session.run("SELECT * FROM t").await.is_err());

    // The new name resolves and carries the data over.
    assert!(db.table_names().contains(&"u".to_string()));
    let batches = session.run("SELECT * FROM u").await.unwrap();
    assert_eq!(total_rows(&batches), 2);
}

#[tokio::test]
async fn alter_table_rename_retargets_triggers() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, seen BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN \
             INSERT INTO audit (id, seen) VALUES (NEW.id, NEW.v); \
             END",
        )
        .await
        .unwrap();

    session.run("ALTER TABLE t RENAME TO u").await.unwrap();
    session
        .run("INSERT INTO u (id, v) VALUES (1, 99)")
        .await
        .unwrap();

    let trigger = db.trigger("t_ai").unwrap();
    assert!(matches!(
        trigger.target,
        TriggerTarget::Table(ref target) if target == "u"
    ));
    let batches = session.run("SELECT seen FROM audit").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![99]);
}

#[tokio::test]
async fn drop_table_removes_target_triggers() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, seen BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER t_ai AFTER INSERT ON t BEGIN \
             INSERT INTO audit (id, seen) VALUES (NEW.id, NEW.v); \
             END",
        )
        .await
        .unwrap();
    assert!(db.trigger("t_ai").is_some());

    session.run("DROP TABLE t").await.unwrap();

    assert!(db.trigger("t_ai").is_none());
    let batches = session.run("PRAGMA trigger_list").await.unwrap();
    assert_eq!(batches[0].num_rows(), 0);
}

#[tokio::test]
async fn alter_table_rename_rejects_conflict_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE a (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    let result = session.run("ALTER TABLE a RENAME TO b").await;
    assert!(
        result.is_err(),
        "renaming onto an existing table name must fail"
    );
    // Both original tables remain intact.
    let mut names = db.table_names();
    names.sort();
    assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
}

#[tokio::test]
async fn alter_table_rename_is_case_insensitive() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();

    session.run("Alter Table t Rename To u").await.unwrap();
    assert_eq!(db.table_names(), vec!["u".to_string()]);
}

#[tokio::test]
async fn alter_table_rename_column_via_sql_refreshes_schema() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    db.transaction(|t| {
        t.put("t", vec![(1, Value::Int64(1)), (2, Value::Int64(10))])?;
        t.put("t", vec![(1, Value::Int64(2)), (2, Value::Int64(20))])
    })
    .unwrap();

    session
        .run("ALTER TABLE t RENAME COLUMN v TO amount")
        .await
        .unwrap();

    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("amount")
        .is_some());
    assert!(session.run("SELECT v FROM t").await.is_err());
    let batches = session.run("SELECT amount FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 2);
    assert_eq!(batches[0].schema().field(0).name(), "amount");
}

#[tokio::test]
async fn alter_table_rename_column_rewrites_trigger_update_of_metadata() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, seen BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER t_v_au AFTER UPDATE OF v ON t BEGIN \
             INSERT INTO audit (id, seen) VALUES (NEW.id, NEW.v); \
             END",
        )
        .await
        .unwrap();
    session
        .run("INSERT INTO t (id, v) VALUES (1, 10)")
        .await
        .unwrap();

    session
        .run("ALTER TABLE t RENAME COLUMN v TO amount")
        .await
        .unwrap();
    session
        .run("UPDATE t SET amount = 42 WHERE id = 1")
        .await
        .unwrap();

    let trigger = db.trigger("t_v_au").unwrap();
    assert_eq!(trigger.update_of, vec!["amount".to_string()]);
    let batches = session.run("SELECT seen FROM audit").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![42]);
}

#[tokio::test]
async fn alter_column_nullability_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    session
        .run("ALTER TABLE t ALTER COLUMN v DROP NOT NULL")
        .await
        .unwrap();
    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("v")
        .unwrap()
        .flags
        .contains(ColumnFlags::NULLABLE));

    db.transaction(|t| t.put("t", vec![(1, Value::Int64(1))]))
        .unwrap();
    let result = session
        .run("ALTER TABLE t ALTER COLUMN v SET NOT NULL")
        .await;
    assert!(
        result.is_err(),
        "SET NOT NULL must reject existing NULL values"
    );
}

#[tokio::test]
async fn alter_column_type_via_sql_on_empty_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)")
        .await
        .unwrap();

    session
        .run("ALTER TABLE t ALTER COLUMN v TYPE TEXT")
        .await
        .unwrap();

    assert_eq!(
        db.table("t")
            .unwrap()
            .lock()
            .schema()
            .column("v")
            .unwrap()
            .ty,
        TypeId::Bytes
    );
    let batches = session.run("SELECT v FROM t").await.unwrap();
    assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn insert_update_delete_and_truncate_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO items (id, name, qty) VALUES \
             (1, 'pencil', 5), (2, 'pen', 8), (3, 'eraser', 2)",
        )
        .await
        .unwrap();

    let batches = session
        .run("SELECT id, name, qty FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);
    assert_eq!(
        string_values(&batches, 1),
        vec![
            "pencil".to_string(),
            "pen".to_string(),
            "eraser".to_string()
        ]
    );
    assert_eq!(i64_values(&batches, 2), vec![5, 8, 2]);

    session
        .run("UPDATE items SET qty = 18 WHERE name = 'pen' OR id = 3")
        .await
        .unwrap();
    let batches = session
        .run("SELECT qty FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![5, 18, 18]);

    session
        .run("DELETE FROM items WHERE qty >= 18 AND name IN ('pen', 'eraser')")
        .await
        .unwrap();
    let batches = session
        .run("SELECT id FROM items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);

    session.run("TRUNCATE TABLE items").await.unwrap();
    let batches = session.run("SELECT id FROM items").await.unwrap();
    assert_eq!(total_rows(&batches), 0);
}

#[tokio::test]
async fn insert_on_conflict_variants_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items (id, name, qty) VALUES (1, 'old', 10)")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO items (id, name, qty) VALUES (1, 'ignored', 99) \
             ON CONFLICT (id) DO NOTHING",
        )
        .await
        .unwrap();

    let batches = session
        .run("SELECT name, qty FROM items WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["old".to_string()]);
    assert_eq!(i64_values(&batches, 1), vec![10]);

    session
        .run(
            "INSERT INTO items (id, name, qty) VALUES (1, 'new', 15) \
             ON CONFLICT (id) DO UPDATE SET name = excluded.name, qty = excluded.qty",
        )
        .await
        .unwrap();
    let batches = session
        .run("SELECT name, qty FROM items WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["new".to_string()]);
    assert_eq!(i64_values(&batches, 1), vec![15]);
}

#[tokio::test]
async fn create_and_drop_index_via_sql_preserves_rows() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE metrics (id BIGINT PRIMARY KEY, category TEXT, amount BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO metrics (id, category, amount) VALUES \
             (1, 'a', 10), (2, 'b', 20), (3, 'a', 30)",
        )
        .await
        .unwrap();

    session
        .run("CREATE INDEX idx_metrics_category ON metrics (category)")
        .await
        .unwrap();
    {
        let schema = db.table("metrics").unwrap().lock().schema().clone();
        assert_eq!(schema.indexes.len(), 1);
        assert_eq!(schema.indexes[0].name, "idx_metrics_category");
    }

    let batches = session
        .run("SELECT id FROM metrics WHERE category = 'a' ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    session
        .run("DROP INDEX idx_metrics_category ON metrics")
        .await
        .unwrap();
    let schema = db.table("metrics").unwrap().lock().schema().clone();
    assert!(schema.indexes.is_empty());
    let batches = session
        .run("SELECT id FROM metrics ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);
}

#[tokio::test]
async fn create_and_drop_view_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, qty BIGINT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO items (id, name, qty) VALUES (1, 'small', 1), (2, 'large', 20)")
        .await
        .unwrap();
    session
        .run("CREATE VIEW large_items AS SELECT name FROM items WHERE qty >= 10")
        .await
        .unwrap();

    let batches = session.run("SELECT name FROM large_items").await.unwrap();
    assert_eq!(string_values(&batches, 0), vec!["large".to_string()]);

    session
        .run("CREATE VIEW item_input(id, name, qty) AS SELECT id, name, qty FROM items")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER item_input_ioi INSTEAD OF INSERT ON item_input BEGIN \
             INSERT INTO items (id, name, qty) VALUES (NEW.id, NEW.name, NEW.qty); \
             END",
        )
        .await
        .unwrap();
    assert!(db.trigger("item_input_ioi").is_some());
    session.run("DROP VIEW item_input").await.unwrap();
    assert!(db.trigger("item_input_ioi").is_none());

    session.run("DROP VIEW large_items").await.unwrap();
    assert!(session.run("SELECT name FROM large_items").await.is_err());
}

#[tokio::test]
async fn introspection_and_admin_commands_via_sql() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE alpha (id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE beta (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("CREATE INDEX idx_alpha_note ON alpha(note)")
        .await
        .unwrap();
    session
        .run("INSERT INTO alpha (id, note) VALUES (10, 'first'), (11, 'second')")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER alpha_ai AFTER INSERT ON alpha BEGIN \
             INSERT INTO beta (id) VALUES (NEW.id); \
             END",
        )
        .await
        .unwrap();
    let batches = session
        .run("SELECT changes(), total_changes(), last_insert_rowid()")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);
    assert_eq!(i64_values(&batches, 1), vec![2]);
    assert_eq!(i64_values(&batches, 2), vec![11]);
    session
        .run("UPDATE alpha SET note = 'changed' WHERE id = 10")
        .await
        .unwrap();
    let batches = session
        .run("SELECT changes(), total_changes()")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    assert_eq!(i64_values(&batches, 1), vec![3]);

    let batches = session.run("SHOW TABLES").await.unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["alpha".to_string(), "beta".to_string()]
    );

    let batches = session.run("DESCRIBE alpha").await.unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["id".to_string(), "note".to_string()]
    );
    assert_eq!(bool_values(&batches, 3), vec![true, false]);

    let batches = session.run("PRAGMA table_info(alpha)").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec!["id".to_string(), "note".to_string()]
    );
    assert_eq!(i64_values(&batches, 5), vec![1, 0]);

    let batches = session.run("PRAGMA table_info('alpha')").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec!["id".to_string(), "note".to_string()]
    );

    let batches = session.run("PRAGMA table_xinfo(alpha)").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec!["id".to_string(), "note".to_string()]
    );
    assert_eq!(i64_values(&batches, 6), vec![0, 0]);

    let batches = session.run("PRAGMA table_list").await.unwrap();
    assert!(string_values(&batches, 1).contains(&"alpha".to_string()));
    assert!(string_values(&batches, 1).contains(&"beta".to_string()));

    let batches = session.run("PRAGMA index_list(alpha)").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec!["idx_alpha_note".to_string()]
    );

    let batches = session
        .run("PRAGMA index_info(idx_alpha_note)")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 2), vec!["note".to_string()]);

    let batches = session
        .run("PRAGMA index_xinfo(idx_alpha_note)")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 2), vec!["note".to_string()]);
    assert_eq!(string_values(&batches, 4), vec!["BINARY".to_string()]);
    assert_eq!(i64_values(&batches, 5), vec![1]);

    let batches = session.run("PRAGMA database_list").await.unwrap();
    assert_eq!(string_values(&batches, 1), vec!["main".to_string()]);

    let batches = session.run("PRAGMA function_list").await.unwrap();
    let functions = string_values(&batches, 0);
    assert!(functions.contains(&"json_each".to_string()));
    assert!(functions.contains(&"json_tree".to_string()));
    assert!(functions.contains(&"jsonb_tree".to_string()));
    assert!(functions.contains(&"group_concat".to_string()));
    assert!(functions.contains(&"median".to_string()));
    assert!(functions.contains(&"percentile".to_string()));
    assert!(functions.contains(&"percentile_cont".to_string()));
    assert!(functions.contains(&"percentile_disc".to_string()));
    assert!(functions.contains(&"total".to_string()));
    assert!(functions.contains(&"rtree_intersects".to_string()));

    let batches = session.run("PRAGMA module_list").await.unwrap();
    let modules = string_values(&batches, 0);
    for module in [
        "dbstat",
        "fts_docs",
        "json_each",
        "json_tree",
        "jsonb_each",
        "jsonb_tree",
        "kv_store",
        "rtree_rects",
        "schema_tables",
        "series",
    ] {
        assert!(modules.contains(&module.to_string()), "{modules:?}");
    }

    let batches = session.run("PRAGMA trigger_list").await.unwrap();
    assert_eq!(string_values(&batches, 1), vec!["alpha_ai".to_string()]);
    assert_eq!(string_values(&batches, 2), vec!["table".to_string()]);
    assert_eq!(string_values(&batches, 3), vec!["alpha".to_string()]);
    assert_eq!(string_values(&batches, 4), vec!["AFTER".to_string()]);
    assert_eq!(string_values(&batches, 5), vec!["INSERT".to_string()]);
    assert_eq!(i64_values(&batches, 6), vec![1]);

    let batches = session.run("PRAGMA collation_list").await.unwrap();
    assert_eq!(string_values(&batches, 1), vec!["BINARY".to_string()]);

    let batches = session.run("PRAGMA compile_options").await.unwrap();
    assert!(string_values(&batches, 0).contains(&"EXTENDED_SQL_FUNCTIONS".to_string()));

    let batches = session
        .run("PRAGMA definitely_unknown_pragma")
        .await
        .unwrap();
    assert_eq!(batches[0].num_columns(), 0);
    assert_eq!(batches[0].num_rows(), 0);

    let batches = session.run("PRAGMA integrity_check").await.unwrap();
    assert_eq!(string_values(&batches, 0), vec!["ok".to_string()]);
    let batches = session.run("PRAGMA foreign_key_check").await.unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "table");
    assert_eq!(batches[0].num_rows(), 0);

    let batches = session.run("PRAGMA schema_version").await.unwrap();
    assert!(i64_values(&batches, 0)[0] > 0);

    let batches = session.run("PRAGMA user_version = 42").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![42]);
    let batches = session.run("PRAGMA application_id = 77").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![77]);

    let batches = session.run("PRAGMA data_version").await.unwrap();
    assert!(i64_values(&batches, 0)[0] > 0);

    let batches = session.run("PRAGMA page_size").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![4096]);
    let batches = session.run("PRAGMA page_count").await.unwrap();
    assert!(i64_values(&batches, 0)[0] >= 0);
    let batches = session.run("PRAGMA wal_checkpoint").await.unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "busy");

    let batches = session.run("CHECK").await.unwrap();
    assert_eq!(batches[0].schema().field(0).name(), "severity");
    session.run("ANALYZE").await.unwrap();
    session.run("PRAGMA optimize").await.unwrap();
    session.run("REINDEX idx_alpha_note").await.unwrap();
    let backup_parent = tempdir().unwrap();
    let backup = backup_parent.path().join("backup");
    session
        .run(&format!("VACUUM INTO '{}'", backup.display()))
        .await
        .unwrap();
    assert!(backup.exists());
    session.run("VACUUM").await.unwrap();

    drop(session);
    drop(db);
    let reopened = Arc::new(Database::open(dir.path()).unwrap());
    let reopened_session = MongrelSession::open(reopened).unwrap();
    let batches = reopened_session.run("PRAGMA user_version").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![42]);
    let batches = reopened_session.run("PRAGMA application_id").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![77]);
}

#[tokio::test]
async fn explicit_transactions_stage_sql_dml() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE tx_items (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO tx_items (id, name) VALUES (1, 'one')")
        .await
        .unwrap();
    session
        .run("INSERT INTO tx_items (id, name) VALUES (2, 'two')")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();

    let batches = session
        .run("SELECT id FROM tx_items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);

    session.run("BEGIN").await.unwrap();
    session
        .run("DELETE FROM tx_items WHERE id = 1")
        .await
        .unwrap();
    session.run("ROLLBACK").await.unwrap();
    let batches = session
        .run("SELECT id FROM tx_items ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);
}

#[tokio::test]
async fn alter_table_add_and_drop_column_via_sql_preserves_rows() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE t (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session.run("INSERT INTO t (id) VALUES (1)").await.unwrap();

    session
        .run("ALTER TABLE t ADD COLUMN note TEXT")
        .await
        .unwrap();
    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("note")
        .is_some());
    session
        .run("INSERT INTO t (id, note) VALUES (2, 'kept')")
        .await
        .unwrap();

    session.run("ALTER TABLE t DROP COLUMN note").await.unwrap();
    assert!(db
        .table("t")
        .unwrap()
        .lock()
        .schema()
        .column("note")
        .is_none());
    assert!(session.run("SELECT note FROM t").await.is_err());
    let batches = session.run("SELECT id FROM t ORDER BY id").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);
}

#[tokio::test]
async fn after_insert_trigger_writes_audit_row_atomically() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, user_id BIGINT, action TEXT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN \
             INSERT INTO audit (id, user_id, action) VALUES (NEW.id, NEW.id, 'insert'); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (7, 'Ada')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT user_id, action FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![7]);
    assert_eq!(string_values(&batches, 1), vec!["insert".to_string()]);
}

#[tokio::test]
async fn create_trigger_if_not_exists_is_idempotent() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, user_id BIGINT, action TEXT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER IF NOT EXISTS users_ai AFTER INSERT ON users BEGIN \
             INSERT INTO audit (id, user_id, action) VALUES (NEW.id, NEW.id, 'first'); \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER IF NOT EXISTS users_ai AFTER INSERT ON users BEGIN \
             INSERT INTO audit (id, user_id, action) VALUES (NEW.id, NEW.id, 'second'); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT action FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["first".to_string()]);
}

#[tokio::test]
async fn trigger_raise_fail_and_rollback_abort_atomically() {
    for action in ["FAIL", "ROLLBACK"] {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(Arc::clone(&db)).unwrap();

        session
            .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        session
            .run(&format!(
                "CREATE TRIGGER users_block AFTER INSERT ON users BEGIN \
                 SELECT RAISE({action}, 'blocked by {action}'); \
                 END"
            ))
            .await
            .unwrap();

        let err = session
            .run("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("blocked by"), "{msg}");

        let batches = session.run("SELECT COUNT(*) FROM users").await.unwrap();
        assert_eq!(i64_values(&batches, 0), vec![0]);
    }
}

#[tokio::test]
async fn before_insert_trigger_writes_side_effect_before_commit() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, user_id BIGINT, action TEXT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_bi BEFORE INSERT ON users BEGIN \
             INSERT INTO audit (id, user_id, action) VALUES (NEW.id, NEW.id, 'before'); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (8, 'Grace')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT user_id, action FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![8]);
    assert_eq!(string_values(&batches, 1), vec!["before".to_string()]);
}

#[tokio::test]
async fn before_trigger_set_new_rewrites_insert_row_via_ir() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    db.create_trigger(
        StoredTrigger::new(
            "users_rewrite",
            TriggerDefinition {
                target: TriggerTarget::Table("users".to_string()),
                timing: TriggerTiming::Before,
                event: TriggerEvent::Insert,
                update_of: Vec::new(),
                target_columns: Vec::new(),
                when: None,
                program: TriggerProgram {
                    steps: vec![TriggerStep::SetNew {
                        cells: vec![TriggerCell {
                            column_id: 2,
                            value: TriggerValue::Literal(Value::Bytes(b"rewritten".to_vec())),
                        }],
                    }],
                },
            },
            0,
        )
        .unwrap(),
    )
    .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (9, 'original')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT name FROM users WHERE id = 9")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["rewritten".to_string()]);
}

#[tokio::test]
async fn trigger_raise_aborts_entire_statement() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_block AFTER INSERT ON users WHEN NEW.id = 2 BEGIN \
             SELECT RAISE(ABORT, 'blocked user'); \
             END",
        )
        .await
        .unwrap();

    let result = session
        .run("INSERT INTO users (id, name) VALUES (2, 'blocked')")
        .await;
    assert!(result.is_err());

    let batches = session.run("SELECT COUNT(*) FROM users").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![0]);
}

#[tokio::test]
async fn trigger_raise_ignore_skips_current_row_and_later_triggers() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_skip BEFORE INSERT ON users WHEN NEW.id = 2 BEGIN \
             INSERT INTO audit (id, note) VALUES (2, 'before ignore'); \
             SELECT RAISE(IGNORE); \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_later BEFORE INSERT ON users WHEN NEW.id = 2 BEGIN \
             INSERT INTO audit (id, note) VALUES (20, 'should not run'); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Skip'), (3, 'Grace')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);
    let batches = session
        .run("SELECT id, note FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["before ignore".to_string()]
    );
}

#[tokio::test]
async fn trigger_when_supports_not_predicates() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, user_id BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_ai AFTER INSERT ON users WHEN NOT (NEW.name = 'skip') BEGIN \
             INSERT INTO audit (id, user_id) VALUES (NEW.id, NEW.id); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'skip'), (3, 'Grace')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT user_id FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);
}

#[tokio::test]
async fn trigger_writes_recover_atomically_after_reopen() {
    let dir = tempdir().unwrap();
    {
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let session = MongrelSession::open(Arc::clone(&db)).unwrap();

        session
            .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        session
            .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, user_id BIGINT)")
            .await
            .unwrap();
        session
            .run(
                "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN \
                 INSERT INTO audit (id, user_id) VALUES (NEW.id, NEW.id); \
                 END",
            )
            .await
            .unwrap();
        session
            .run(
                "CREATE TRIGGER users_block AFTER INSERT ON users WHEN NEW.id = 2 BEGIN \
                 INSERT INTO audit (id, user_id) VALUES (20, NEW.id); \
                 SELECT RAISE(ABORT, 'blocked'); \
                 END",
            )
            .await
            .unwrap();

        session
            .run("INSERT INTO users (id, name) VALUES (1, 'Ada')")
            .await
            .unwrap();
        assert!(session
            .run("INSERT INTO users (id, name) VALUES (2, 'Blocked')")
            .await
            .is_err());
    }

    let reopened_db = Arc::new(Database::open(dir.path()).unwrap());
    let reopened = MongrelSession::open(reopened_db).unwrap();
    let batches = reopened
        .run("SELECT id FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    let batches = reopened
        .run("SELECT id, user_id FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    assert_eq!(i64_values(&batches, 1), vec![1]);
}

#[tokio::test]
async fn instead_of_insert_trigger_routes_view_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_ioi INSTEAD OF INSERT ON user_names BEGIN \
             INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO user_names (id, name) VALUES (10, 'Ada'), (11, 'Grace')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT id, name FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![10, 11]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["Ada".to_string(), "Grace".to_string()]
    );
}

#[tokio::test]
async fn instead_of_trigger_raise_ignore_stops_later_view_triggers_for_row() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_skip INSTEAD OF INSERT ON user_names WHEN NEW.id = 2 BEGIN \
             SELECT RAISE(IGNORE); \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_route INSTEAD OF INSERT ON user_names BEGIN \
             INSERT INTO users (id, name) VALUES (NEW.id, NEW.name); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO user_names (id, name) VALUES (1, 'Ada'), (2, 'Skip'), (3, 'Grace')")
        .await
        .unwrap();

    let batches = session
        .run("SELECT id, name FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["Ada".to_string(), "Grace".to_string()]
    );
}

#[tokio::test]
async fn insert_into_view_without_instead_of_trigger_is_rejected() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();

    let result = session
        .run("INSERT INTO user_names (id, name) VALUES (12, 'Nope')")
        .await;
    assert!(result.is_err());

    let batches = session.run("SELECT COUNT(*) FROM users").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![0]);
}

#[tokio::test]
async fn instead_of_update_trigger_routes_view_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO users (id, name) VALUES (20, 'Old'), (21, 'Keep')")
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_iou INSTEAD OF UPDATE ON user_names BEGIN \
             UPDATE users SET name = NEW.name WHERE id = OLD.id; \
             END",
        )
        .await
        .unwrap();

    session
        .run("UPDATE user_names SET name = 'New' WHERE id = 20")
        .await
        .unwrap();

    let batches = session
        .run("SELECT id, name FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![20, 21]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["New".to_string(), "Keep".to_string()]
    );
}

#[tokio::test]
async fn instead_of_delete_trigger_routes_view_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO users (id, name) VALUES (30, 'Drop'), (31, 'Keep')")
        .await
        .unwrap();
    session
        .run("CREATE VIEW user_names (id, name) AS SELECT id, name FROM users")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_names_iod INSTEAD OF DELETE ON user_names BEGIN \
             DELETE FROM users WHERE id = OLD.id; \
             END",
        )
        .await
        .unwrap();

    session
        .run("DELETE FROM user_names WHERE name = 'Drop'")
        .await
        .unwrap();

    let batches = session
        .run("SELECT id, name FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![31]);
    assert_eq!(string_values(&batches, 1), vec!["Keep".to_string()]);
}

#[tokio::test]
async fn instead_of_delete_trigger_routes_join_view_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE notes (user_id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO users (id, name) VALUES (40, 'Drop'), (41, 'Keep')")
        .await
        .unwrap();
    session
        .run("INSERT INTO notes (user_id, note) VALUES (40, 'stale'), (41, 'live')")
        .await
        .unwrap();
    session
        .run(
            "CREATE VIEW user_notes (id, name, note) AS \
             SELECT u.id, u.name, n.note \
             FROM users AS u JOIN notes AS n ON u.id = n.user_id",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER user_notes_iod INSTEAD OF DELETE ON user_notes BEGIN \
             DELETE FROM notes WHERE user_id = OLD.id; \
             DELETE FROM users WHERE id = OLD.id; \
             END",
        )
        .await
        .unwrap();

    session
        .run("DELETE FROM user_notes WHERE note = 'stale'")
        .await
        .unwrap();

    let users = session
        .run("SELECT id, name FROM users ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&users, 0), vec![41]);
    assert_eq!(string_values(&users, 1), vec!["Keep".to_string()]);

    let notes = session
        .run("SELECT user_id, note FROM notes ORDER BY user_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&notes, 0), vec![41]);
    assert_eq!(string_values(&notes, 1), vec!["live".to_string()]);
}

#[tokio::test]
async fn recursive_triggers_are_configurable_and_bounded_by_default_off() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, user_id BIGINT)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE log (id BIGINT PRIMARY KEY, audit_id BIGINT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN \
             INSERT INTO audit (id, user_id) VALUES (NEW.id, NEW.id); \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER audit_ai AFTER INSERT ON audit BEGIN \
             INSERT INTO log (id, audit_id) VALUES (NEW.id, NEW.id); \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO users (id, name) VALUES (1, 'one')")
        .await
        .unwrap();
    let batches = session.run("SELECT COUNT(*) FROM audit").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    let batches = session.run("SELECT COUNT(*) FROM log").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![0]);

    let batches = session.run("PRAGMA recursive_triggers = 1").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);

    session
        .run("INSERT INTO users (id, name) VALUES (2, 'two')")
        .await
        .unwrap();
    let batches = session.run("SELECT audit_id FROM log").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);
}

#[tokio::test]
async fn recursive_trigger_cycles_report_trigger_stack() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE a (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("CREATE TABLE b (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER a_ai AFTER INSERT ON a BEGIN \
             INSERT INTO b (id) VALUES (NEW.id); \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER b_ai AFTER INSERT ON b BEGIN \
             INSERT INTO a (id) VALUES (NEW.id); \
             END",
        )
        .await
        .unwrap();
    session.run("PRAGMA recursive_triggers = 1").await.unwrap();

    let err = session
        .run("INSERT INTO a (id) VALUES (1)")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("trigger recursion cycle detected"), "{msg}");
    assert!(msg.contains("a_ai -> b_ai -> a_ai"), "{msg}");

    let batches = session.run("SELECT COUNT(*) FROM a").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![0]);
    let batches = session.run("SELECT COUNT(*) FROM b").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![0]);
}

#[tokio::test]
async fn series_external_module_works_as_function_and_cataloged_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    let (_batches, trace) = session
        .run_sql_traced("SELECT value FROM series(1, 3)")
        .await
        .unwrap();
    assert_eq!(
        trace.scan_mode,
        mongreldb_core::trace::ScanMode::ExternalModule
    );

    let batches = session
        .run("SELECT value FROM series(1, 3) ORDER BY value")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);
    let batches = session
        .run("SELECT value FROM series(1, 1000) LIMIT 3")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);

    session
        .run("CREATE VIRTUAL TABLE nums USING series(1, 3)")
        .await
        .unwrap();
    let (_batches, trace) = session
        .run_sql_traced("SELECT value FROM nums")
        .await
        .unwrap();
    assert_eq!(
        trace.scan_mode,
        mongreldb_core::trace::ScanMode::ExternalModule
    );

    let batches = session
        .run("SELECT value FROM nums ORDER BY value")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);
    let batches = session
        .run("SELECT value FROM nums WHERE value >= 2 AND 3 >= value ORDER BY value")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2, 3]);
    let batches = session
        .run("SELECT value FROM nums ORDER BY value LIMIT 2")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2]);
    let batches = session
        .run("SELECT COUNT(*) FROM nums WHERE value >= 2")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);

    let batches = session.run("PRAGMA table_xinfo(nums)").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec![
            "value".to_string(),
            "start".to_string(),
            "stop".to_string(),
            "step".to_string()
        ]
    );
    assert_eq!(i64_values(&batches, 6), vec![0, 1, 1, 1]);

    let entry = db.external_table("nums").unwrap();
    assert!(entry.capabilities.read_only);
    assert!(entry.capabilities.deterministic);
    assert!(entry.capabilities.trigger_safe);
    assert!(session
        .run("INSERT INTO nums (value) VALUES (4)")
        .await
        .is_err());

    session
        .run("CREATE TABLE users (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    let err = session
        .run(
            "CREATE TRIGGER users_ai AFTER INSERT ON users BEGIN \
             INSERT INTO nums (value) VALUES (NEW.id); \
             END",
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("external table"), "{msg}");
    assert!(msg.contains("not writable"), "{msg}");
}

#[tokio::test]
async fn kv_store_external_module_supports_durable_sql_writes() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE VIRTUAL TABLE kv USING kv_store")
        .await
        .unwrap();
    let entry = db.external_table("kv").unwrap();
    assert!(entry.capabilities.writable);
    assert!(!entry.capabilities.read_only);

    session
        .run("INSERT INTO kv (key, value) VALUES ('one', '1'), ('two', '2')")
        .await
        .unwrap();
    let batches = session
        .run("SELECT key, value FROM kv ORDER BY key")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["one".to_string(), "two".to_string()]
    );
    assert_eq!(
        string_values(&batches, 1),
        vec!["1".to_string(), "2".to_string()]
    );

    session
        .run("UPDATE kv SET value = 'uno' WHERE key = 'one'")
        .await
        .unwrap();
    session
        .run("DELETE FROM kv WHERE key = 'two'")
        .await
        .unwrap();
    let batches = session
        .run("SELECT key, value FROM kv ORDER BY key")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["one".to_string()]);
    assert_eq!(string_values(&batches, 1), vec!["uno".to_string()]);

    let reopened = MongrelSession::open(Arc::clone(&db)).unwrap();
    let batches = reopened
        .run("SELECT key, value FROM kv ORDER BY key")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["one".to_string()]);
    assert_eq!(string_values(&batches, 1), vec!["uno".to_string()]);

    let err = session
        .run("INSERT INTO kv (key, value) VALUES ('one', 'duplicate')")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("primary key conflict"));

    session
        .run("CREATE TABLE audit (id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO kv (key, value) VALUES ('three', '3')")
        .await
        .unwrap();
    session
        .run("UPDATE kv SET value = 'tres' WHERE key = 'three'")
        .await
        .unwrap();
    session
        .run("INSERT INTO audit (id, note) VALUES (1, 'external state commit')")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();
    let batches = session
        .run("SELECT key, value FROM kv ORDER BY key")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["one".to_string(), "three".to_string()]
    );
    assert_eq!(
        string_values(&batches, 1),
        vec!["uno".to_string(), "tres".to_string()]
    );
    let batches = session
        .run("SELECT id, note FROM audit ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["external state commit".to_string()]
    );

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO kv (key, value) VALUES ('rolled', 'back')")
        .await
        .unwrap();
    session.run("ROLLBACK").await.unwrap();
    let batches = session
        .run("SELECT key FROM kv WHERE key = 'rolled'")
        .await
        .unwrap();
    assert!(string_values(&batches, 0).is_empty());

    drop(reopened);
    drop(session);
    drop(db);
    std::fs::remove_file(dir.path().join(VTAB_DIR).join("kv").join("state.json")).unwrap();
    let recovered_db = Arc::new(Database::open(dir.path()).unwrap());
    let recovered = MongrelSession::open(recovered_db).unwrap();
    let batches = recovered
        .run("SELECT key, value FROM kv ORDER BY key")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["one".to_string(), "three".to_string()]
    );
    assert_eq!(
        string_values(&batches, 1),
        vec!["uno".to_string(), "tres".to_string()]
    );
}

#[tokio::test]
async fn triggers_can_write_transaction_safe_external_tables() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE VIRTUAL TABLE kv USING kv_store")
        .await
        .unwrap();
    session
        .run("CREATE TABLE events (id BIGINT PRIMARY KEY, event_key TEXT, value TEXT)")
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER events_ai AFTER INSERT ON events BEGIN \
             INSERT INTO kv (key, value) VALUES (NEW.event_key, NEW.value); \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER events_au AFTER UPDATE OF value ON events BEGIN \
             UPDATE kv SET value = NEW.value WHERE key = NEW.event_key; \
             END",
        )
        .await
        .unwrap();
    session
        .run(
            "CREATE TRIGGER events_ad AFTER DELETE ON events BEGIN \
             DELETE FROM kv WHERE key = OLD.event_key; \
             END",
        )
        .await
        .unwrap();

    session
        .run("INSERT INTO events (id, event_key, value) VALUES (1, 'one', '1')")
        .await
        .unwrap();
    let batches = session
        .run("SELECT key, value FROM kv ORDER BY key")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["one".to_string()]);
    assert_eq!(string_values(&batches, 1), vec!["1".to_string()]);

    session
        .run("UPDATE events SET value = 'uno' WHERE id = 1")
        .await
        .unwrap();
    let batches = session
        .run("SELECT value FROM kv WHERE key = 'one'")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["uno".to_string()]);

    session
        .run("DELETE FROM events WHERE id = 1")
        .await
        .unwrap();
    let batches = session.run("SELECT key FROM kv").await.unwrap();
    assert!(string_values(&batches, 0).is_empty());
}

#[tokio::test]
async fn fts_docs_external_module_searches_hidden_query_column() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE VIRTUAL TABLE docs USING fts_docs")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO docs (doc_id, text) VALUES \
             (1, 'Rust database internals'), \
             (2, 'SQLite compatibility notes'), \
             (3, 'Rust query planner')",
        )
        .await
        .unwrap();

    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    let batches = session
        .run("SELECT doc_id FROM docs WHERE docs MATCH 'rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    let batches = session
        .run("SELECT d.doc_id FROM docs AS d WHERE d MATCH 'sqlite' ORDER BY d.doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);

    let batches = session
        .run("SELECT d.doc_id FROM docs d WHERE d.text MATCH 'query' ORDER BY d.doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![3]);

    let batches = session
        .run("SELECT doc_id FROM docs WHERE text MATCH 'rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'rust database' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);

    let batches = session
        .run(
            "SELECT doc_id, rank, snippet, highlight \
             FROM docs WHERE query = 'rust query' ORDER BY rank DESC, doc_id",
        )
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![3]);
    assert_eq!(f64_values(&batches, 1), vec![2.0]);
    assert_eq!(
        string_values(&batches, 2),
        vec!["[Rust] [query] planner".to_string()]
    );
    assert_eq!(
        string_values(&batches, 3),
        vec!["<b>Rust</b> <b>query</b> planner".to_string()]
    );

    session
        .run("UPDATE docs SET text = 'SQLite virtual table module' WHERE doc_id = 2")
        .await
        .unwrap();
    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'virtual module' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);

    session
        .run("DELETE FROM docs WHERE doc_id = 3")
        .await
        .unwrap();
    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);

    let xinfo = session.run("PRAGMA table_xinfo(docs)").await.unwrap();
    assert_eq!(
        string_values(&xinfo, 1),
        vec![
            "doc_id".to_string(),
            "text".to_string(),
            "query".to_string(),
            "rank".to_string(),
            "snippet".to_string(),
            "highlight".to_string()
        ]
    );
    assert_eq!(i64_values(&xinfo, 6), vec![0, 0, 1, 0, 0, 0]);

    let indexes = session.run("PRAGMA index_list(docs)").await.unwrap();
    assert_eq!(
        string_values(&indexes, 1),
        vec!["docs_fts_inverted".to_string()]
    );
    assert_eq!(string_values(&indexes, 3), vec!["m".to_string()]);
    let info = session
        .run("PRAGMA index_info(docs_fts_inverted)")
        .await
        .unwrap();
    assert_eq!(string_values(&info, 2), vec!["text".to_string()]);
}

#[tokio::test]
async fn fts_docs_supports_query_options_and_richer_syntax() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE VIRTUAL TABLE docs USING fts_docs(prefix=1, min_token_len=3, stopwords='the|and')")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO docs (doc_id, text) VALUES \
             (1, 'Rust database internals'), \
             (2, 'SQLite virtual table module'), \
             (3, 'Rust query planner'), \
             (4, 'Database query adapters'), \
             (5, 'The Go language')",
        )
        .await
        .unwrap();

    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'rust OR sqlite' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 2, 3]);

    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'query NOT rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![4]);

    let batches = session
        .run(r#"SELECT doc_id FROM docs WHERE query = '"rust query"' ORDER BY doc_id"#)
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![3]);

    let batches = session
        .run("SELECT doc_id FROM docs WHERE query = 'data*' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 4]);

    session
        .run("CREATE VIRTUAL TABLE case_docs USING fts_docs(case_sensitive=1)")
        .await
        .unwrap();
    session
        .run("INSERT INTO case_docs (doc_id, text) VALUES (1, 'Rust'), (2, 'rust')")
        .await
        .unwrap();
    let batches = session
        .run("SELECT doc_id FROM case_docs WHERE query = 'Rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    let batches = session
        .run("SELECT doc_id FROM case_docs WHERE query = 'rust' ORDER BY doc_id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);
}

#[tokio::test]
async fn rtree_rects_external_module_filters_overlap_bounds() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE VIRTUAL TABLE rects USING rtree_rects")
        .await
        .unwrap();
    session
        .run(
            "INSERT INTO rects (id, min_x, max_x, min_y, max_y) VALUES \
             (1, 0.0, 2.0, 0.0, 2.0), \
             (2, 10.0, 12.0, 10.0, 12.0), \
             (3, 1.5, 4.0, 1.5, 4.0)",
        )
        .await
        .unwrap();

    let batches = session
        .run(
            "SELECT id FROM rects \
             WHERE query_min_x = 1.0 AND query_max_x = 3.0 \
               AND query_min_y = 1.0 AND query_max_y = 3.0 \
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    let batches = session
        .run(
            "SELECT id FROM rects \
             WHERE rtree_intersects(min_x, max_x, min_y, max_y, 1.0, 3.0, 1.0, 3.0) \
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1, 3]);

    session
        .run("UPDATE rects SET min_x = 20.0, max_x = 21.0 WHERE id = 3")
        .await
        .unwrap();
    let batches = session
        .run(
            "SELECT id FROM rects \
             WHERE query_min_x = 1.0 AND query_max_x = 3.0 \
               AND query_min_y = 1.0 AND query_max_y = 3.0 \
             ORDER BY id",
        )
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);

    let xinfo = session.run("PRAGMA table_xinfo(rects)").await.unwrap();
    assert_eq!(
        string_values(&xinfo, 1),
        vec![
            "id".to_string(),
            "min_x".to_string(),
            "max_x".to_string(),
            "min_y".to_string(),
            "max_y".to_string(),
            "query_min_x".to_string(),
            "query_max_x".to_string(),
            "query_min_y".to_string(),
            "query_max_y".to_string()
        ]
    );
    assert_eq!(i64_values(&xinfo, 6), vec![0, 0, 0, 0, 0, 1, 1, 1, 1]);

    let indexes = session.run("PRAGMA index_list(rects)").await.unwrap();
    assert_eq!(
        string_values(&indexes, 1),
        vec!["rects_rtree_spatial".to_string()]
    );
    assert_eq!(string_values(&indexes, 3), vec!["m".to_string()]);
    let info = session
        .run("PRAGMA index_info(rects_rtree_spatial)")
        .await
        .unwrap();
    assert_eq!(
        string_values(&info, 2),
        vec![
            "min_x".to_string(),
            "max_x".to_string(),
            "min_y".to_string(),
            "max_y".to_string()
        ]
    );
}

#[tokio::test]
async fn json_external_modules_work_as_cataloged_tables() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run(r#"CREATE VIRTUAL TABLE je USING json_each('[10,{"a":1}]')"#)
        .await
        .unwrap();
    let batches = session
        .run("SELECT key, value, type, atom FROM je WHERE id = 0")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["0".to_string()]);
    assert_eq!(string_values(&batches, 1), vec!["10".to_string()]);
    assert_eq!(string_values(&batches, 2), vec!["integer".to_string()]);
    assert_eq!(string_values(&batches, 3), vec!["10".to_string()]);

    let batches = session
        .run("SELECT key, value, type FROM je WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["1".to_string()]);
    assert_eq!(string_values(&batches, 1), vec![r#"{"a":1}"#.to_string()]);
    assert_eq!(string_values(&batches, 2), vec!["object".to_string()]);
    let batches = session.run("SELECT COUNT(*) FROM je").await.unwrap();
    assert_eq!(i64_values(&batches, 0), vec![2]);

    session
        .run(r#"CREATE VIRTUAL TABLE jt USING json_tree('{"a":[1,{"b":2}]}')"#)
        .await
        .unwrap();
    let batches = session
        .run("SELECT atom, path FROM jt WHERE fullkey = '$.a[1].b'")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["2".to_string()]);
    assert_eq!(string_values(&batches, 1), vec!["$.a[1]".to_string()]);

    let batches = session.run("PRAGMA table_xinfo(je)").await.unwrap();
    assert_eq!(
        string_values(&batches, 1),
        vec![
            "key".to_string(),
            "value".to_string(),
            "type".to_string(),
            "atom".to_string(),
            "id".to_string(),
            "parent".to_string(),
            "fullkey".to_string(),
            "path".to_string(),
            "json".to_string(),
            "root".to_string()
        ]
    );
    assert_eq!(i64_values(&batches, 6), vec![0, 0, 0, 0, 0, 0, 0, 0, 1, 1]);

    let entry = db.external_table("je").unwrap();
    assert!(entry.capabilities.read_only);
    assert!(entry.capabilities.deterministic);
    assert!(entry.capabilities.trigger_safe);
    assert!(session
        .run("UPDATE je SET value = '11' WHERE id = 0")
        .await
        .is_err());
}

#[tokio::test]
async fn catalog_external_modules_expose_database_metadata() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    session
        .run("CREATE TABLE alpha (id BIGINT PRIMARY KEY, note TEXT)")
        .await
        .unwrap();
    session
        .run("INSERT INTO alpha (id, note) VALUES (1, 'one')")
        .await
        .unwrap();
    session
        .run("CREATE VIRTUAL TABLE schema_catalog USING schema_tables")
        .await
        .unwrap();
    session
        .run("CREATE VIRTUAL TABLE stats USING dbstat")
        .await
        .unwrap();

    let batches = session
        .run("SELECT type, ncol FROM schema_catalog WHERE name = 'alpha'")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["table".to_string()]);
    assert_eq!(i64_values(&batches, 1), vec![2]);

    let batches = session
        .run("SELECT type, module FROM schema_catalog WHERE name = 'schema_catalog'")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["external".to_string()]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["schema_tables".to_string()]
    );

    let batches = session
        .run("SELECT rows, memtable_rows, columns FROM stats WHERE name = 'alpha'")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![1]);
    assert_eq!(i64_values(&batches, 1), vec![1]);
    assert_eq!(i64_values(&batches, 2), vec![2]);

    let reopened = MongrelSession::open(Arc::clone(&db)).unwrap();
    let batches = reopened
        .run("SELECT module FROM schema_catalog WHERE name = 'stats'")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["dbstat".to_string()]);
}

#[tokio::test]
async fn app_registered_external_module_can_back_virtual_table() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    let initial_destroyed = Arc::new(AtomicBool::new(false));
    session
        .register_external_module(Arc::new(AppRowsModule::new(Arc::clone(&initial_destroyed))))
        .unwrap();

    let batches = session.run("PRAGMA module_list").await.unwrap();
    assert!(string_values(&batches, 0).contains(&"app_rows".to_string()));

    session
        .run("CREATE VIRTUAL TABLE app USING app_rows")
        .await
        .unwrap();
    let batches = session
        .run("SELECT id, label FROM app ORDER BY id")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![7, 8]);
    assert_eq!(
        string_values(&batches, 1),
        vec!["seven".to_string(), "eight".to_string()]
    );
    session
        .run("CREATE TABLE picked (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    session
        .run("INSERT INTO picked (id) VALUES (8)")
        .await
        .unwrap();
    let batches = session
        .run("SELECT app.label FROM picked JOIN app ON picked.id = app.id")
        .await
        .unwrap();
    assert_eq!(string_values(&batches, 0), vec!["eight".to_string()]);
    let indexes = session.run("PRAGMA index_list(app)").await.unwrap();
    assert_eq!(
        string_values(&indexes, 1),
        vec!["app_label_lookup".to_string()]
    );
    assert_eq!(string_values(&indexes, 3), vec!["m".to_string()]);
    let info = session
        .run("PRAGMA index_info(app_label_lookup)")
        .await
        .unwrap();
    assert_eq!(string_values(&info, 2), vec!["label".to_string()]);

    let err = session
        .run("INSERT INTO app (id, label) VALUES (9, 'nine')")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("read-only"), "{err}");
    drop(session);

    let reopened_destroyed = Arc::new(AtomicBool::new(false));
    let modules: Vec<Arc<dyn ExternalTableModule>> = vec![Arc::new(AppRowsModule::new(
        Arc::clone(&reopened_destroyed),
    ))];
    let reopened = MongrelSession::open_with_external_modules(Arc::clone(&db), modules).unwrap();
    let batches = reopened
        .run("SELECT label FROM app ORDER BY id")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["seven".to_string(), "eight".to_string()]
    );
    assert!(!initial_destroyed.load(Ordering::SeqCst));
    assert!(!reopened_destroyed.load(Ordering::SeqCst));

    reopened.run("DROP TABLE app").await.unwrap();
    assert!(reopened_destroyed.load(Ordering::SeqCst));
    assert!(!initial_destroyed.load(Ordering::SeqCst));
    let err = reopened.run("SELECT * FROM app").await.unwrap_err();
    assert!(
        err.to_string().contains("not found") || err.to_string().contains("not exist"),
        "{err}"
    );
}

#[tokio::test]
async fn deterministic_read_only_external_module_plans_are_cached() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    let destroyed = Arc::new(AtomicBool::new(false));
    let plan_calls = Arc::new(AtomicUsize::new(0));
    session
        .register_external_module(Arc::new(AppRowsModule::with_plan_counter(
            Arc::clone(&destroyed),
            Arc::clone(&plan_calls),
        )))
        .unwrap();
    session
        .run("CREATE VIRTUAL TABLE app USING app_rows")
        .await
        .unwrap();

    let batches = session
        .run("SELECT id FROM app WHERE label = 'seven'")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![7]);
    let calls_after_first = plan_calls.load(Ordering::SeqCst);
    assert!(
        calls_after_first > 0,
        "expected DataFusion to negotiate external filter pushdown"
    );

    session.clear_cache();
    let batches = session
        .run("SELECT id FROM app WHERE label = 'seven'")
        .await
        .unwrap();
    assert_eq!(i64_values(&batches, 0), vec![7]);
    assert_eq!(plan_calls.load(Ordering::SeqCst), calls_after_first);
}

#[tokio::test]
async fn external_module_errors_are_typed_schema_errors() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();

    let err = session
        .run("CREATE VIRTUAL TABLE missing USING no_such_module")
        .await
        .unwrap_err();
    assert!(
        matches!(err, MongrelQueryError::Schema(ref message) if message.contains("not registered")),
        "{err}"
    );

    let err = session
        .run("CREATE VIRTUAL TABLE bad_series USING series(1, 2, 3, 4)")
        .await
        .unwrap_err();
    assert!(
        matches!(err, MongrelQueryError::Schema(ref message) if message.contains("at most three arguments")),
        "{err}"
    );
}

#[tokio::test]
async fn app_registered_writable_external_module_uses_external_txn_state() {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    let session = MongrelSession::open(Arc::clone(&db)).unwrap();
    session
        .register_external_module(Arc::new(AppTxnModule))
        .unwrap();

    session
        .run("CREATE VIRTUAL TABLE app_state USING app_txn")
        .await
        .unwrap();
    session
        .run("INSERT INTO app_state (key, value) VALUES ('one', 'uno'), ('two', 'dos')")
        .await
        .unwrap();
    let batches = session
        .run("SELECT key, value FROM app_state ORDER BY key")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["one".to_string(), "two".to_string()]
    );
    assert_eq!(
        string_values(&batches, 1),
        vec!["uno".to_string(), "dos".to_string()]
    );

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO app_state (key, value) VALUES ('rolled', 'back')")
        .await
        .unwrap();
    session.run("ROLLBACK").await.unwrap();
    let batches = session
        .run("SELECT key FROM app_state WHERE key = 'rolled'")
        .await
        .unwrap();
    assert!(string_values(&batches, 0).is_empty());

    session.run("BEGIN").await.unwrap();
    session
        .run("INSERT INTO app_state (key, value) VALUES ('three', 'tres')")
        .await
        .unwrap();
    session.run("COMMIT").await.unwrap();
    drop(session);

    let modules: Vec<Arc<dyn ExternalTableModule>> = vec![Arc::new(AppTxnModule)];
    let reopened = MongrelSession::open_with_external_modules(Arc::clone(&db), modules).unwrap();
    let batches = reopened
        .run("SELECT key, value FROM app_state ORDER BY key")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["one".to_string(), "three".to_string(), "two".to_string()]
    );
    assert_eq!(
        string_values(&batches, 1),
        vec!["uno".to_string(), "tres".to_string(), "dos".to_string()]
    );

    reopened
        .run("UPDATE app_state SET value = 'due' WHERE key = 'two'")
        .await
        .unwrap();
    reopened
        .run("DELETE FROM app_state WHERE key = 'one'")
        .await
        .unwrap();
    let batches = reopened
        .run("SELECT key, value FROM app_state ORDER BY key")
        .await
        .unwrap();
    assert_eq!(
        string_values(&batches, 0),
        vec!["three".to_string(), "two".to_string()]
    );
    assert_eq!(
        string_values(&batches, 1),
        vec!["tres".to_string(), "due".to_string()]
    );
}
