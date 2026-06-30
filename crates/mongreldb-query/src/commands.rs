use crate::{MongrelProvider, MongrelQueryError, MongrelSession, Result};
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::rowid::RowId;
use mongreldb_core::schema::{
    AlterColumn, ColumnDef as CoreColumnDef, ColumnFlags, IndexDef, IndexKind,
    Schema as CoreSchema, TypeId,
};
use mongreldb_core::Database;
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, Assignment, AssignmentTarget,
    BinaryOperator, ColumnDef, ColumnOption, CreateIndex, CreateTable, CreateView, DataType,
    Delete, Expr, FromTable, Ident, IndexColumn, Insert, ObjectName, ObjectType, OnConflictAction,
    OnInsert, Query, RenameTableNameKind, SetExpr, Statement, TableConstraint, TableFactor,
    TableObject, TableWithJoins, Truncate, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) enum PendingSqlOp {
    Put {
        table: String,
        cells: Vec<(u16, Value)>,
    },
    Delete {
        table: String,
        row_id: RowId,
    },
}

pub(crate) fn try_run_command(
    session: &MongrelSession,
    sql: &str,
) -> Result<Option<Vec<RecordBatch>>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let lower = trimmed.to_ascii_lowercase();
    if let Some(batch) = try_manual_command(session, trimmed, &lower)? {
        return Ok(Some(batch));
    }
    if !should_parse(&lower) {
        return Ok(None);
    }

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, trimmed)
        .map_err(|e| MongrelQueryError::Schema(format!("SQL parse error: {e}")))?;
    if statements.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "only one statement may be executed at a time".into(),
        ));
    }

    let Some(db) = session.database.as_ref() else {
        return Ok(None);
    };

    let out = match statements.into_iter().next().unwrap() {
        Statement::CreateTable(create) => {
            create_table(session, db, create)?;
            Vec::new()
        }
        Statement::Drop {
            object_type,
            if_exists,
            names,
            table,
            ..
        } => match object_type {
            ObjectType::Table => {
                for name in names {
                    drop_table(session, db, &object_name(&name)?, if_exists)?;
                }
                Vec::new()
            }
            ObjectType::View | ObjectType::MaterializedView => {
                for name in names {
                    session.drop_view(&object_name(&name)?);
                }
                Vec::new()
            }
            ObjectType::Index => {
                drop_index(session, db, names, table, if_exists)?;
                Vec::new()
            }
            _ => return Ok(None),
        },
        Statement::AlterTable(alter) => {
            alter_table(session, db, alter)?;
            Vec::new()
        }
        Statement::CreateIndex(index) => {
            create_index(session, db, index)?;
            Vec::new()
        }
        Statement::CreateView(view) => {
            create_view(session, view)?;
            Vec::new()
        }
        Statement::Insert(insert) => {
            insert_rows(session, db, insert)?;
            Vec::new()
        }
        Statement::Update(update) => {
            update_rows(session, db, update)?;
            Vec::new()
        }
        Statement::Delete(delete) => {
            delete_rows(session, db, delete)?;
            Vec::new()
        }
        Statement::Truncate(truncate) => {
            truncate_tables(session, db, truncate)?;
            Vec::new()
        }
        Statement::StartTransaction { .. } => {
            let mut staged = session.sql_txn.lock();
            if staged.is_some() {
                return Err(MongrelQueryError::Schema(
                    "a SQL transaction is already open".into(),
                ));
            }
            *staged = Some(Vec::new());
            Vec::new()
        }
        Statement::Commit { .. } => {
            let ops = session.sql_txn.lock().take().unwrap_or_default();
            apply_ops(db, ops)?;
            session.clear_cache();
            Vec::new()
        }
        Statement::Rollback { .. } => {
            session.sql_txn.lock().take();
            Vec::new()
        }
        Statement::Analyze(_) => Vec::new(),
        Statement::Vacuum(_) => {
            compact_all(db)?;
            session.clear_cache();
            Vec::new()
        }
        _ => return Ok(None),
    };

    Ok(Some(out))
}

fn should_parse(lower: &str) -> bool {
    [
        "create table",
        "drop table",
        "alter table",
        "create index",
        "drop index",
        "create view",
        "drop view",
        "create materialized view",
        "drop materialized view",
        "insert",
        "replace",
        "update",
        "delete",
        "truncate",
        "begin",
        "start transaction",
        "commit",
        "rollback",
        "analyze",
        "vacuum",
    ]
    .iter()
    .any(|prefix| lower.starts_with(prefix))
}

fn try_manual_command(
    session: &MongrelSession,
    sql: &str,
    lower: &str,
) -> Result<Option<Vec<RecordBatch>>> {
    let Some(db) = session.database.as_ref() else {
        return Ok(None);
    };

    if lower == "show tables" || lower == "show table" {
        let mut names = db.table_names();
        names.sort();
        return Ok(Some(vec![strings_batch("table_name", names)?]));
    }

    if let Some(table) = lower
        .strip_prefix("describe ")
        .or_else(|| lower.strip_prefix("desc "))
    {
        let table = strip_identifier(table.trim())?;
        return Ok(Some(vec![describe_table(db, table)?]));
    }

    if let Some(inner) = lower.strip_prefix("pragma table_info") {
        let table = inner
            .trim()
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))
            .ok_or_else(|| {
                MongrelQueryError::Schema("expected PRAGMA table_info(<table>)".into())
            })?;
        let table = strip_identifier(table.trim())?;
        return Ok(Some(vec![pragma_table_info(db, table)?]));
    }

    if lower == "check" || lower == "check database" || lower == "pragma integrity_check" {
        return Ok(Some(vec![check_batch(db)?]));
    }

    if lower == "doctor" || lower == "doctor database" {
        let quarantined = db.doctor()?;
        let values: Vec<String> = quarantined.into_iter().map(|id| id.to_string()).collect();
        session.clear_cache();
        return Ok(Some(vec![strings_batch("quarantined_table_id", values)?]));
    }

    if lower == "compact" || lower == "compact database" || lower == "vacuum" {
        compact_all(db)?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if sql.ends_with(';') {
        return try_manual_command(
            session,
            sql.trim_end_matches(';').trim(),
            lower.trim_end_matches(';'),
        );
    }

    Ok(None)
}

fn create_table(session: &MongrelSession, db: &Arc<Database>, create: CreateTable) -> Result<()> {
    let name = object_name(&create.name)?;
    if create.if_not_exists && db.table_id(&name).is_ok() {
        return Ok(());
    }
    if create.query.is_some() {
        return Err(MongrelQueryError::Schema(
            "CREATE TABLE AS SELECT is not supported by MongrelDB SQL DDL".into(),
        ));
    }
    let schema = schema_from_create_table(&create)?;
    db.create_table(&name, schema)?;
    register_table(session, db, &name)?;
    session.clear_cache();
    Ok(())
}

fn drop_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: &str,
    if_exists: bool,
) -> Result<()> {
    match db.drop_table(name) {
        Ok(()) => {
            let _ = session.ctx.deregister_table(name);
            session.tables.lock().remove(name);
            session.clear_cache();
            Ok(())
        }
        Err(e) if if_exists && matches!(e, mongreldb_core::MongrelError::NotFound(_)) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn create_view(session: &MongrelSession, view: CreateView) -> Result<()> {
    if view.materialized {
        return Err(MongrelQueryError::Schema(
            "CREATE MATERIALIZED VIEW is not supported; use CREATE VIEW".into(),
        ));
    }
    let name = object_name(&view.name)?;
    session.create_view(&name, &view.query.to_string());
    Ok(())
}

fn alter_table(session: &MongrelSession, db: &Arc<Database>, alter: AlterTable) -> Result<()> {
    let table_name = object_name(&alter.name)?;
    if alter.operations.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "ALTER TABLE currently supports one operation per statement".into(),
        ));
    }
    match alter.operations.into_iter().next().unwrap() {
        AlterTableOperation::RenameTable {
            table_name: new_name,
        } => {
            let new_name = match new_name {
                RenameTableNameKind::As(n) | RenameTableNameKind::To(n) => object_name(&n)?,
            };
            db.rename_table(&table_name, &new_name)?;
            let _ = session.ctx.deregister_table(&table_name);
            session.tables.lock().remove(&table_name);
            register_table(session, db, &new_name)?;
        }
        AlterTableOperation::RenameColumn {
            old_column_name,
            new_column_name,
        } => {
            db.alter_column(
                &table_name,
                &old_column_name.value,
                AlterColumn::rename(new_column_name.value),
            )?;
            session.refresh_registered_table(db, &table_name)?;
        }
        AlterTableOperation::AlterColumn { column_name, op } => {
            alter_column(session, db, &table_name, column_name, op)?;
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
            rebuild_table(session, db, &table_name, schema)?;
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
            rebuild_table(session, db, &table_name, schema)?;
        }
        AlterTableOperation::DropIndex { name } => {
            let mut schema = table_schema(db, &table_name)?;
            remove_index_defs(&mut schema, &name.value);
            rebuild_table(session, db, &table_name, schema)?;
        }
        AlterTableOperation::AddConstraint { constraint, .. } => {
            add_table_constraint(session, db, &table_name, constraint)?;
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
            rebuild_table(session, db, &table_name, schema)?;
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
) -> Result<()> {
    match op {
        AlterColumnOperation::SetNotNull => {
            let flags =
                current_column_flags(db, table, &column.value)?.without(ColumnFlags::NULLABLE);
            db.alter_column(table, &column.value, AlterColumn::set_flags(flags))?;
        }
        AlterColumnOperation::DropNotNull => {
            let flags = current_column_flags(db, table, &column.value)?.with(ColumnFlags::NULLABLE);
            db.alter_column(table, &column.value, AlterColumn::set_flags(flags))?;
        }
        AlterColumnOperation::SetDataType { data_type, .. } => {
            db.alter_column(
                table,
                &column.value,
                AlterColumn::set_type(sql_type_to_core(&data_type)?),
            )?;
        }
        AlterColumnOperation::SetDefault { .. } | AlterColumnOperation::DropDefault => {
            return Err(MongrelQueryError::Schema(
                "column defaults are not persisted by MongrelDB core SQL DDL".into(),
            ));
        }
        other => {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported ALTER COLUMN operation: {other}"
            )));
        }
    }
    session.refresh_registered_table(db, table)?;
    Ok(())
}

fn add_table_constraint(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    constraint: TableConstraint,
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
            add_index_defs(&mut schema, &name, idx.columns, index_kind_from_sql(idx.index_type.as_ref())?)?;
            rebuild_table(session, db, table, schema)
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
            rebuild_table(session, db, table, schema)
        }
        TableConstraint::PrimaryKey(pk) => Err(MongrelQueryError::Schema(format!(
            "adding primary keys after table creation is not supported: {pk}"
        ))),
        TableConstraint::Unique(_)
        | TableConstraint::ForeignKey(_)
        | TableConstraint::Check(_)
        | TableConstraint::PrimaryKeyUsingIndex(_)
        | TableConstraint::UniqueUsingIndex(_) => Err(MongrelQueryError::Schema(
            "UNIQUE, FOREIGN KEY, and CHECK enforcement is provided by MongrelDB Kit, not core SQL DDL".into(),
        )),
    }
}

fn create_index(session: &MongrelSession, db: &Arc<Database>, index: CreateIndex) -> Result<()> {
    if index.unique {
        return Err(MongrelQueryError::Schema(
            "CREATE UNIQUE INDEX is not supported by core SQL; use MongrelDB Kit unique constraints".into(),
        ));
    }
    if index.predicate.is_some() {
        return Err(MongrelQueryError::Schema(
            "partial indexes are not supported".into(),
        ));
    }
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
    add_index_defs(
        &mut schema,
        &name,
        index.columns,
        index_kind_from_sql(index.using.as_ref())?,
    )?;
    rebuild_table(session, db, &table, schema)?;
    session.clear_cache();
    Ok(())
}

fn drop_index(
    session: &MongrelSession,
    db: &Arc<Database>,
    names: Vec<ObjectName>,
    table: Option<ObjectName>,
    if_exists: bool,
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
        rebuild_table(session, db, &table_name, schema)?;
    }
    session.clear_cache();
    Ok(())
}

fn insert_rows(session: &MongrelSession, db: &Arc<Database>, insert: Insert) -> Result<()> {
    let table = match insert.table {
        TableObject::TableName(name) => object_name(&name)?,
        _ => {
            return Err(MongrelQueryError::Schema(
                "INSERT target must be a table name".into(),
            ));
        }
    };
    if insert.returning.is_some() {
        return Err(MongrelQueryError::Schema(
            "INSERT RETURNING is not supported".into(),
        ));
    }
    let schema = table_schema(db, &table)?;
    let columns = insert_columns(&schema, &insert.columns)?;
    let rows = values_rows(insert.source.as_deref())?;
    let mut ops = Vec::new();
    for row in rows {
        if row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "INSERT has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let mut cells = Vec::with_capacity(row.len());
        for (col, expr) in columns.iter().zip(row.iter()) {
            cells.push((col.id, expr_to_value(expr, col.ty)?));
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
                    });
                }
                OnConflictAction::DoUpdate(update) => {
                    if let Some(existing) = pk_conflict_row(db, &table, &schema, &cells)? {
                        let excluded = cells_to_map(&cells);
                        let mut merged = existing.columns.clone();
                        for assignment in &update.assignments {
                            apply_assignment(&schema, &mut merged, assignment, Some(&excluded))?;
                        }
                        ops.push(PendingSqlOp::Delete {
                            table: table.clone(),
                            row_id: existing.row_id,
                        });
                        ops.push(PendingSqlOp::Put {
                            table: table.clone(),
                            cells: map_to_cells(&merged),
                        });
                    } else {
                        ops.push(PendingSqlOp::Put {
                            table: table.clone(),
                            cells,
                        });
                    }
                }
            },
            Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
                if let Some(existing) = pk_conflict_row(db, &table, &schema, &cells)? {
                    let excluded = cells_to_map(&cells);
                    let mut merged = existing.columns.clone();
                    for assignment in assignments {
                        apply_assignment(&schema, &mut merged, assignment, Some(&excluded))?;
                    }
                    ops.push(PendingSqlOp::Delete {
                        table: table.clone(),
                        row_id: existing.row_id,
                    });
                    ops.push(PendingSqlOp::Put {
                        table: table.clone(),
                        cells: map_to_cells(&merged),
                    });
                } else {
                    ops.push(PendingSqlOp::Put {
                        table: table.clone(),
                        cells,
                    });
                }
            }
            None => ops.push(PendingSqlOp::Put {
                table: table.clone(),
                cells,
            }),
            Some(_) => {
                return Err(MongrelQueryError::Schema(
                    "this INSERT conflict action is not supported".into(),
                ));
            }
        }
    }
    stage_or_apply(session, db, ops)
}

fn update_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    update: sqlparser::ast::Update,
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
    let (schema, rows) = visible_rows(db, &table)?;
    let mut ops = Vec::new();
    for row in rows {
        if predicate_matches(update.selection.as_ref(), &schema, &row)? {
            let mut merged = row.columns.clone();
            for assignment in &update.assignments {
                apply_assignment(&schema, &mut merged, assignment, None)?;
            }
            ops.push(PendingSqlOp::Delete {
                table: table.clone(),
                row_id: row.row_id,
            });
            ops.push(PendingSqlOp::Put {
                table: table.clone(),
                cells: map_to_cells(&merged),
            });
        }
    }
    stage_or_apply(session, db, ops)
}

fn delete_rows(session: &MongrelSession, db: &Arc<Database>, delete: Delete) -> Result<()> {
    if delete.returning.is_some()
        || delete.output.is_some()
        || delete.using.is_some()
        || !delete.tables.is_empty()
        || !delete.order_by.is_empty()
        || delete.limit.is_some()
    {
        return Err(MongrelQueryError::Schema(
            "DELETE USING/RETURNING/ORDER BY/LIMIT and multi-table DELETE are not supported".into(),
        ));
    }
    let table = single_from_table(&delete.from)?;
    let (schema, rows) = visible_rows(db, &table)?;
    let ops = rows
        .into_iter()
        .filter_map(
            |row| match predicate_matches(delete.selection.as_ref(), &schema, &row) {
                Ok(true) => Some(Ok(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id: row.row_id,
                })),
                Ok(false) => None,
                Err(e) => Some(Err(e)),
            },
        )
        .collect::<Result<Vec<_>>>()?;
    stage_or_apply(session, db, ops)
}

fn truncate_tables(session: &MongrelSession, db: &Arc<Database>, truncate: Truncate) -> Result<()> {
    let mut ops = Vec::new();
    for target in truncate.table_names {
        let table = object_name(&target.name)?;
        match visible_rows(db, &table) {
            Ok((_, rows)) => {
                ops.extend(rows.into_iter().map(|row| PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id: row.row_id,
                }));
            }
            Err(e)
                if truncate.if_exists
                    && matches!(
                        e,
                        MongrelQueryError::Core(mongreldb_core::MongrelError::NotFound(_))
                    ) => {}
            Err(e) => return Err(e),
        }
    }
    stage_or_apply(session, db, ops)
}

fn stage_or_apply(
    session: &MongrelSession,
    db: &Arc<Database>,
    ops: Vec<PendingSqlOp>,
) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }
    if let Some(staged) = session.sql_txn.lock().as_mut() {
        staged.extend(ops);
        return Ok(());
    }
    apply_ops(db, ops)?;
    session.clear_cache();
    Ok(())
}

fn apply_ops(db: &Arc<Database>, ops: Vec<PendingSqlOp>) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }
    db.transaction(|tx| {
        for op in ops {
            match op {
                PendingSqlOp::Put { table, cells } => {
                    tx.put(&table, cells)?;
                }
                PendingSqlOp::Delete { table, row_id } => {
                    tx.delete(&table, row_id)?;
                }
            }
        }
        Ok(())
    })?;
    Ok(())
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
            TableConstraint::Unique(_)
            | TableConstraint::ForeignKey(_)
            | TableConstraint::Check(_)
            | TableConstraint::PrimaryKeyUsingIndex(_)
            | TableConstraint::UniqueUsingIndex(_)
            | TableConstraint::FulltextOrSpatial(_) => {
                return Err(MongrelQueryError::Schema(
                    "UNIQUE, FOREIGN KEY, CHECK, and SPATIAL constraints are not enforced by core SQL DDL; use MongrelDB Kit for those constraints".into(),
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
            _ => {}
        }
    }
    schema.validate_auto_increment()?;
    Ok(schema)
}

fn core_column_from_sql(
    id: u16,
    col: &ColumnDef,
    table_primary_key: bool,
) -> Result<CoreColumnDef> {
    let mut flags = ColumnFlags::empty().with(ColumnFlags::NULLABLE);
    let mut primary_key = table_primary_key;
    let mut auto_increment = false;
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
            ColumnOption::Unique(_) | ColumnOption::ForeignKey(_) | ColumnOption::Check(_) => {
                return Err(MongrelQueryError::Schema(
                    "column UNIQUE, REFERENCES, and CHECK constraints are provided by MongrelDB Kit, not core SQL DDL".into(),
                ));
            }
            ColumnOption::Default(_) => {
                return Err(MongrelQueryError::Schema(
                    "column defaults are not persisted by MongrelDB core SQL DDL".into(),
                ));
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
    })
}

fn sql_type_to_core(data_type: &DataType) -> Result<TypeId> {
    let text = data_type.to_string().to_ascii_lowercase();
    let base = text.split('(').next().unwrap_or(text.as_str()).trim();
    match base {
        "bigint" | "int8" | "int64" | "integer" | "int" | "int4" | "smallint" | "int2"
        | "tinyint" | "mediumint" => Ok(TypeId::Int64),
        "double" | "double precision" | "float8" | "float64" | "real" | "float" => {
            Ok(TypeId::Float64)
        }
        "varchar" | "character varying" | "char varying" | "text" | "string" | "bytes"
        | "bytea" | "blob" | "varbinary" => Ok(TypeId::Bytes),
        "boolean" | "bool" => Ok(TypeId::Bool),
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
    schema: &CoreSchema,
    row: &mut HashMap<u16, Value>,
    assignment: &Assignment,
    excluded: Option<&HashMap<u16, Value>>,
) -> Result<()> {
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
    let value = eval_value_expr(&assignment.value, schema, row, excluded)?;
    row.insert(column.id, coerce_value(value, column.ty)?);
    Ok(())
}

fn predicate_matches(selection: Option<&Expr>, schema: &CoreSchema, row: &Row) -> Result<bool> {
    match selection {
        Some(expr) => eval_bool_expr(expr, schema, &row.columns),
        None => Ok(true),
    }
}

fn eval_bool_expr(expr: &Expr, schema: &CoreSchema, row: &HashMap<u16, Value>) -> Result<bool> {
    match expr {
        Expr::Nested(e) => eval_bool_expr(e, schema, row),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(!eval_bool_expr(expr, schema, row)?),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                Ok(eval_bool_expr(left, schema, row)? && eval_bool_expr(right, schema, row)?)
            }
            BinaryOperator::Or => {
                Ok(eval_bool_expr(left, schema, row)? || eval_bool_expr(right, schema, row)?)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                let l = eval_value_expr(left, schema, row, None)?;
                let r = eval_value_expr(right, schema, row, None)?;
                compare_values(&l, op, &r)
            }
            _ => Err(MongrelQueryError::Schema(format!(
                "unsupported predicate operator: {op}"
            ))),
        },
        Expr::IsNull(e) => Ok(matches!(
            eval_value_expr(e, schema, row, None)?,
            Value::Null
        )),
        Expr::IsNotNull(e) => Ok(!matches!(
            eval_value_expr(e, schema, row, None)?,
            Value::Null
        )),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = eval_value_expr(expr, schema, row, None)?;
            let lo = eval_value_expr(low, schema, row, None)?;
            let hi = eval_value_expr(high, schema, row, None)?;
            let result = compare_values(&value, &BinaryOperator::GtEq, &lo)?
                && compare_values(&value, &BinaryOperator::LtEq, &hi)?;
            Ok(if *negated { !result } else { result })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_value_expr(expr, schema, row, None)?;
            let mut found = false;
            for candidate in list {
                if compare_values(
                    &value,
                    &BinaryOperator::Eq,
                    &eval_value_expr(candidate, schema, row, None)?,
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
            let value = eval_value_expr(expr, schema, row, None)?;
            let pattern = eval_value_expr(pattern, schema, row, None)?;
            let (Value::Bytes(value), Value::Bytes(pattern)) = (value, pattern) else {
                return Ok(false);
            };
            let value = String::from_utf8_lossy(&value);
            let pattern = String::from_utf8_lossy(&pattern);
            let result = like_match(&value, &pattern);
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
) -> Result<Value> {
    match expr {
        Expr::Nested(e) => eval_value_expr(e, schema, row, excluded),
        Expr::Value(v) => sql_value_to_value(&v.value, None),
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
        } => match eval_value_expr(expr, schema, row, excluded)? {
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
        Expr::Value(v) => sql_value_to_value(&v.value, Some(ty))?,
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr_to_value(expr, ty)? {
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
        _ => unreachable!(),
    })
}

fn like_match(value: &str, pattern: &str) -> bool {
    fn rec(v: &[char], p: &[char]) -> bool {
        match p.split_first() {
            None => v.is_empty(),
            Some(('%', rest)) => rec(v, rest) || (!v.is_empty() && rec(&v[1..], p)),
            Some(('_', rest)) => !v.is_empty() && rec(&v[1..], rest),
            Some((ch, rest)) => v.first() == Some(ch) && rec(&v[1..], rest),
        }
    }
    rec(
        &value.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
    )
}

fn visible_rows(db: &Arc<Database>, table: &str) -> Result<(CoreSchema, Vec<Row>)> {
    let handle = db.table(table)?;
    let guard = handle.lock();
    let schema = guard.schema().clone();
    let snapshot = guard.snapshot();
    let rows = guard.visible_rows(snapshot)?;
    Ok((schema, rows))
}

fn table_schema(db: &Arc<Database>, table: &str) -> Result<CoreSchema> {
    let handle = db.table(table)?;
    let schema = {
        let guard = handle.lock();
        guard.schema().clone()
    };
    Ok(schema)
}

fn rebuild_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
    new_schema: CoreSchema,
) -> Result<()> {
    let (_, rows) = visible_rows(db, table)?;
    let temp = format!(
        "__mongrel_tmp_rebuild_{}_{}",
        table,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    db.create_table(&temp, new_schema.clone())?;
    let temp_result = apply_ops(
        db,
        rows.iter()
            .map(|row| PendingSqlOp::Put {
                table: temp.clone(),
                cells: row_to_schema_cells(row, &new_schema),
            })
            .collect(),
    );
    if let Err(e) = temp_result {
        let _ = db.drop_table(&temp);
        return Err(e);
    }

    db.drop_table(table)?;
    db.create_table(table, new_schema.clone())?;
    apply_ops(
        db,
        rows.iter()
            .map(|row| PendingSqlOp::Put {
                table: table.to_string(),
                cells: row_to_schema_cells(row, &new_schema),
            })
            .collect(),
    )?;
    let _ = db.drop_table(&temp);
    let _ = session.ctx.deregister_table(table);
    session.tables.lock().remove(table);
    register_table(session, db, table)?;
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
    let provider = MongrelProvider::new(handle.clone())?;
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
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported index type: {other}"
        ))),
    }
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
    let guard = handle.lock();
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
    Ok(value.trim_matches('"').trim_matches('`'))
}

fn compact_all(db: &Arc<Database>) -> Result<()> {
    for table in db.table_names() {
        db.table(&table)?.lock().compact()?;
    }
    let _ = db.gc()?;
    Ok(())
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
    let not_null: Vec<bool> = schema
        .columns
        .iter()
        .map(|c| !c.flags.contains(ColumnFlags::NULLABLE))
        .collect();
    let pk: Vec<bool> = schema
        .columns
        .iter()
        .map(|c| c.flags.contains(ColumnFlags::PRIMARY_KEY))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("notnull", ArrowDataType::Boolean, false),
            Field::new("pk", ArrowDataType::Boolean, false),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(BooleanArray::from(not_null)),
            Arc::new(BooleanArray::from(pk)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn check_batch(db: &Arc<Database>) -> Result<RecordBatch> {
    let issues = db.check();
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
