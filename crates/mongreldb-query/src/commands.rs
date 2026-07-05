use crate::{
    ExternalBaseWrite, ExternalModuleIndex, ExternalModuleRegistry, ExternalWriteOp,
    MongrelProvider, MongrelQueryError, MongrelSession, Result,
};
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use mongreldb_core::memtable::{Row, Value};
use mongreldb_core::procedure::{ProcedureCallOutput, ProcedureCallRow, StoredProcedure};
use mongreldb_core::rowid::RowId;
use mongreldb_core::schema::{
    AlterColumn, ColumnDef as CoreColumnDef, ColumnFlags, IndexDef, IndexKind,
    Schema as CoreSchema, TypeId,
};
use mongreldb_core::Database;
use mongreldb_core::{
    ExternalTableDefinition, ExternalTableEntry, ExternalTriggerBaseWrite, ExternalTriggerBridge,
    ExternalTriggerWrite, ExternalTriggerWriteResult, ModuleArg, StoredTrigger, TriggerCell,
    TriggerDefinition, TriggerEvent, TriggerExpr, TriggerProgram, TriggerRaiseAction, TriggerStep,
    TriggerTarget, TriggerTiming, TriggerValue,
};
use sqlparser::ast::{
    AlterColumnOperation, AlterTable, AlterTableOperation, Assignment, AssignmentTarget,
    BinaryOperator, ColumnDef, ColumnOption, ConditionalStatements, CreateIndex, CreateTable,
    CreateTrigger, CreateView, DataType, Delete, DropTrigger, Expr, FromTable, FunctionArg,
    FunctionArgExpr, FunctionArguments, Ident, IndexColumn, Insert, ObjectName, ObjectType,
    OnConflictAction, OnInsert, Query, RenameTableNameKind, SetExpr, Statement, TableConstraint,
    TableFactor, TableObject, TableWithJoins, TriggerEvent as SqlTriggerEvent, TriggerObject,
    TriggerObjectKind, TriggerPeriod, Truncate, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
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
    ExternalState {
        table: String,
        state: Vec<u8>,
        changes: u64,
    },
}

pub(crate) async fn try_run_command(
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
        Statement::CreateVirtualTable {
            name,
            if_not_exists,
            module_name,
            module_args,
        } => {
            create_virtual_table(
                session,
                db,
                name,
                if_not_exists,
                module_name.value,
                module_args.into_iter().map(|arg| arg.value).collect(),
            )?;
            Vec::new()
        }
        Statement::CreateTrigger(trigger) => {
            create_trigger(session, db, trigger)?;
            session.clear_cache();
            Vec::new()
        }
        Statement::DropTrigger(drop) => {
            drop_trigger(db, drop)?;
            session.clear_cache();
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
                    drop_view(session, db, &object_name(&name)?, if_exists)?;
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
            update_rows(session, db, update).await?;
            Vec::new()
        }
        Statement::Delete(delete) => {
            delete_rows(session, db, delete).await?;
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
            let changes = logical_changes(&ops);
            let external_tables = external_tables_to_refresh(db, &ops);
            apply_ops(session, db, ops)?;
            refresh_external_tables(session, db, &external_tables)?;
            session.sql_fn_state.record_changes(changes, None);
            session.clear_cache();
            Vec::new()
        }
        Statement::Rollback { .. } => {
            session.sql_txn.lock().take();
            Vec::new()
        }
        Statement::Analyze(_) => {
            analyze_all(db)?;
            session.clear_cache();
            Vec::new()
        }
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
        "create virtual table",
        "create trigger",
        "create or replace trigger",
        "drop table",
        "drop trigger",
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
    // ATTACH/DETACH don't require a primary Database — they mount external ones.
    if lower.starts_with("attach ") {
        return attach_database(session, sql);
    }
    if lower.starts_with("detach ") {
        return detach_database(session, sql);
    }

    // SAVEPOINT / RELEASE / ROLLBACK TO — operate on the session's SQL staging.
    if let Some(rest) = lower.strip_prefix("savepoint ") {
        let name = rest.trim().trim_end_matches(';').trim().to_string();
        let staged = session.sql_txn.lock();
        let mut sp = session.savepoints.lock();
        let len = staged.as_ref().map_or(0, |v| v.len());
        sp.push((name, len));
        return Ok(Some(Vec::new()));
    }
    if let Some(rest) = lower.strip_prefix("release ") {
        let name = rest
            .trim()
            .strip_prefix("savepoint ")
            .unwrap_or(rest.trim())
            .trim_end_matches(';')
            .trim()
            .to_string();
        let mut sp = session.savepoints.lock();
        if name.is_empty() {
            sp.pop();
        } else if let Some(pos) = sp.iter().rposition(|(n, _)| n == &name) {
            sp.truncate(pos);
        }
        return Ok(Some(Vec::new()));
    }
    if let Some(rest) = lower.strip_prefix("rollback to ") {
        let name = rest
            .trim()
            .strip_prefix("savepoint ")
            .unwrap_or(rest.trim())
            .trim_end_matches(';')
            .trim()
            .to_string();
        let mut sp = session.savepoints.lock();
        let staged = session.sql_txn.lock();
        let target_len = sp
            .iter()
            .rposition(|(n, _)| n == &name)
            .map(|pos| {
                let len = sp[pos].1;
                sp.truncate(pos);
                len
            })
            .ok_or_else(|| {
                MongrelQueryError::Schema(format!("no savepoint named '{name}'"))
            })?;
        if let Some(ops) = staged.as_ref() {
            let mut ops = ops.clone();
            ops.truncate(target_len);
            *session.sql_txn.lock() = Some(ops);
        }
        return Ok(Some(Vec::new()));
    }

    let Some(db) = session.database.as_ref() else {
        return Ok(None);
    };

    if lower == "show tables" || lower == "show table" {
        let mut names = db.table_names();
        names.sort();
        return Ok(Some(vec![strings_batch("table_name", names)?]));
    }

    if lower.starts_with("create virtual table ") {
        create_virtual_table_manual(session, db, sql)?;
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("create trigger if not exists ") {
        create_trigger_if_not_exists_manual(session, db, sql)?;
        return Ok(Some(Vec::new()));
    }

    if lower == "show procedures" || lower == "show procedure" {
        let mut names: Vec<String> = db.procedures().into_iter().map(|p| p.name).collect();
        names.sort();
        return Ok(Some(vec![strings_batch("procedure_name", names)?]));
    }

    if let Some(name) = lower
        .strip_prefix("describe procedure ")
        .or_else(|| lower.strip_prefix("desc procedure "))
    {
        let name = strip_identifier(name)?;
        let procedure = db
            .procedure(name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("procedure {name:?} not found")))?;
        return Ok(Some(vec![json_batch(
            "procedure_json",
            vec![serde_json::to_string(&procedure)
                .map_err(|e| MongrelQueryError::Schema(e.to_string()))?],
        )?]));
    }

    if lower.starts_with("create or replace procedure ") {
        let (name, json) = parse_procedure_json(sql, lower, "create or replace procedure ")?;
        let procedure = procedure_from_json(name, json)?;
        db.create_or_replace_procedure(procedure)?;
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("create procedure ") {
        let (name, json) = parse_procedure_json(sql, lower, "create procedure ")?;
        let procedure = procedure_from_json(name, json)?;
        db.create_procedure(procedure)?;
        return Ok(Some(Vec::new()));
    }

    if let Some(name) = lower.strip_prefix("drop procedure ") {
        let name = strip_identifier(name)?;
        db.drop_procedure(name)?;
        return Ok(Some(Vec::new()));
    }

    if lower.starts_with("call ") {
        let (name, args) = parse_call_json(sql, lower)?;
        let result = db.call_procedure(name, args)?;
        let json = serde_json::to_string(&procedure_output_json(&result.output))
            .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        session.clear_cache();
        return Ok(Some(vec![json_batch("result_json", vec![json])?]));
    }

    if let Some(table) = lower
        .strip_prefix("describe ")
        .or_else(|| lower.strip_prefix("desc "))
    {
        let table = strip_identifier(table.trim())?;
        return Ok(Some(vec![describe_table(db, table)?]));
    }

    if lower.starts_with("pragma ") {
        return Ok(Some(vec![run_pragma(session, db, sql, lower)?]));
    }

    if lower == "check" || lower == "check database" {
        return Ok(Some(vec![check_batch(db)?]));
    }

    if lower == "doctor" || lower == "doctor database" {
        let quarantined = db.doctor()?;
        let values: Vec<String> = quarantined.into_iter().map(|id| id.to_string()).collect();
        session.clear_cache();
        return Ok(Some(vec![strings_batch("quarantined_table_id", values)?]));
    }

    if lower.starts_with("vacuum into ") {
        let target = parse_vacuum_into(sql, lower)?;
        compact_all(db)?;
        copy_database_dir(db.root(), Path::new(target))?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if lower == "compact" || lower == "compact database" || lower == "vacuum" {
        compact_all(db)?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if lower == "analyze" || lower.starts_with("analyze ") {
        analyze_all(db)?;
        session.clear_cache();
        return Ok(Some(Vec::new()));
    }

    if lower == "reindex" || lower.starts_with("reindex ") {
        let target = lower
            .strip_prefix("reindex")
            .map(str::trim)
            .unwrap_or_default();
        let target = if target.is_empty() {
            None
        } else {
            Some(strip_identifier(target)?)
        };
        reindex(db, target)?;
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

/// `ATTACH 'path' AS alias` — open a second MongrelDB database directory and
/// register all its tables on the session's DataFusion context, prefixed with
/// `alias.`. This enables cross-database SQL joins (e.g.
/// `SELECT * FROM alias.users JOIN local.orders`).
fn attach_database(session: &MongrelSession, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    // Parse: ATTACH 'path' AS alias  (also: ATTACH DATABASE 'path' AS alias)
    let lower = sql.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("attach ")
        .unwrap_or("")
        .strip_prefix("database ")
        .unwrap_or("");
    // Extract quoted path and AS alias.
    let (path, alias) = parse_attach_args(rest, sql)?;
    let attached_db = Arc::new(Database::open(&path)?);
    let table_names = attached_db.table_names();
    for name in &table_names {
        let handle = attached_db.table(name)?;
        let provider = MongrelProvider::new(handle.clone())?;
        // Register under a qualified name `{alias}_{name}` (using underscore,
        // not dot — DataFusion's `schema.table` resolution requires catalog
        // setup for dot-qualified names). This avoids collisions with tables
        // from the primary database.
        let qualified = format!("{alias}_{name}");
        session
            .ctx
            .register_table(&qualified, Arc::new(provider))
            .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
        session.tables.lock().insert(qualified, handle);
        // Also register the bare name if no collision exists.
        let bare_provider = MongrelProvider::new(attached_db.table(name)?)?;
        if session
            .ctx
            .register_table(name, Arc::new(bare_provider))
            .is_ok()
        {
            session
                .tables
                .lock()
                .insert(name.clone(), attached_db.table(name)?);
        }
    }
    // Store the attached Database so it stays alive for the session's lifetime.
    session
        .attached_databases
        .lock()
        .insert(alias.clone(), attached_db);
    session.clear_cache();
    Ok(Some(Vec::new()))
}

/// `DETACH alias` — unregister all tables from the attached database.
fn detach_database(session: &MongrelSession, sql: &str) -> Result<Option<Vec<RecordBatch>>> {
    let lower = sql.to_ascii_lowercase();
    let rest = lower.strip_prefix("detach ").unwrap_or("");
    let alias = parse_detach_alias(rest)?;
    let removed = session.attached_databases.lock().remove(&alias);
    if removed.is_none() {
        // DETACH of a non-attached alias is a no-op (SQLite: error, but we
        // tolerate it for idempotency).
        return Ok(Some(Vec::new()));
    }
    // Remove qualified and bare table registrations belonging to this alias.
    let mut tables = session.tables.lock();
    let to_remove: Vec<String> = tables
        .keys()
        .filter(|k| k.starts_with(&format!("{alias}_")))
        .cloned()
        .collect();
    for name in &to_remove {
        tables.remove(name);
        let _ = session.ctx.deregister_table(name);
    }
    session.clear_cache();
    Ok(Some(Vec::new()))
}

/// Extract the path and alias from `ATTACH ... 'path' AS alias` arguments.
fn parse_attach_args(_lower_rest: &str, original_sql: &str) -> Result<(PathBuf, String)> {
    // Parse entirely from the original SQL (not the lowercased version) to avoid
    // any case/whitespace surprises. Pattern: ATTACH [DATABASE] 'path' AS alias
    let sql_lower = original_sql.to_ascii_lowercase();
    // Find the single-quoted path.
    let path_start = original_sql.find('\'').ok_or_else(|| {
        MongrelQueryError::Schema("ATTACH requires a quoted path: ATTACH 'path' AS alias".into())
    })?;
    let path_end = original_sql[path_start + 1..]
        .find('\'')
        .ok_or_else(|| MongrelQueryError::Schema("ATTACH path missing closing quote".into()))?;
    let path = PathBuf::from(&original_sql[path_start + 1..path_start + 1 + path_end]);
    // Find the alias: everything after the closing quote, split on " as "
    // (case-insensitive). The alias is the last token, trimmed of semicolons.
    let after_path = &sql_lower[path_start + 1 + path_end + 1..];
    let alias_part = if let Some(as_pos) = after_path.find(" as ") {
        &after_path[as_pos + 4..]
    } else {
        // Also accept the SQL keyword without spaces around it.
        after_path.trim_start()
    };
    let alias = alias_part.trim().trim_end_matches(';').trim().to_string();
    if alias.is_empty() {
        return Err(MongrelQueryError::Schema(
            "ATTACH requires AS alias: ATTACH 'path' AS alias".into(),
        ));
    }
    Ok((path, alias))
}

fn parse_detach_alias(lower_rest: &str) -> Result<String> {
    let alias = lower_rest
        .strip_prefix("database ")
        .unwrap_or(lower_rest)
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();
    if alias.is_empty() {
        return Err(MongrelQueryError::Schema("DETACH requires an alias".into()));
    }
    Ok(alias)
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

fn create_virtual_table_manual(
    session: &MongrelSession,
    db: &Arc<Database>,
    sql: &str,
) -> Result<()> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let lower = sql.to_ascii_lowercase();
    let mut rest = lower
        .strip_prefix("create virtual table ")
        .ok_or_else(|| MongrelQueryError::Schema("expected CREATE VIRTUAL TABLE".into()))?;
    let mut if_not_exists = false;
    let original_rest_start = "create virtual table ".len();
    let mut original_rest = &sql[original_rest_start..];
    if rest.starts_with("if not exists ") {
        if_not_exists = true;
        rest = &rest["if not exists ".len()..];
        original_rest = &original_rest["if not exists ".len()..];
    }
    let Some(using_pos) = rest.find(" using ") else {
        return Err(MongrelQueryError::Schema(
            "CREATE VIRTUAL TABLE requires USING module".into(),
        ));
    };
    let name = strip_identifier(original_rest[..using_pos].trim())?.to_string();
    let after_using = original_rest[using_pos + " using ".len()..].trim();
    let (module, args) = if let Some(open) = after_using.find('(') {
        let close = after_using
            .rfind(')')
            .ok_or_else(|| MongrelQueryError::Schema("CREATE VIRTUAL TABLE missing ')'".into()))?;
        let module = strip_identifier(after_using[..open].trim())?.to_string();
        let args = split_module_args(&after_using[open + 1..close])?;
        (module, args)
    } else {
        (strip_identifier(after_using)?.to_string(), Vec::new())
    };
    create_virtual_table_named(session, db, name, if_not_exists, module, args)
}

fn split_module_args(raw: &str) -> Result<Vec<String>> {
    let mut args = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut expected_closers = Vec::new();
    let mut chars = raw.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if let Some(active_quote) = quote {
            if ch == active_quote {
                if chars.peek().is_some_and(|(_, next)| *next == active_quote) {
                    chars.next();
                } else {
                    quote = None;
                }
            }
            continue;
        }

        match ch {
            '\'' | '"' | '`' => quote = Some(ch),
            '(' => expected_closers.push(')'),
            '[' => expected_closers.push(']'),
            '{' => expected_closers.push('}'),
            ')' | ']' | '}' => {
                if expected_closers.pop() != Some(ch) {
                    return Err(MongrelQueryError::Schema(
                        "unbalanced CREATE VIRTUAL TABLE module argument delimiters".into(),
                    ));
                }
            }
            ',' if expected_closers.is_empty() => {
                push_module_arg(&mut args, &raw[start..idx])?;
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }

    if quote.is_some() {
        return Err(MongrelQueryError::Schema(
            "unterminated CREATE VIRTUAL TABLE module argument string".into(),
        ));
    }
    if !expected_closers.is_empty() {
        return Err(MongrelQueryError::Schema(
            "unbalanced CREATE VIRTUAL TABLE module argument delimiters".into(),
        ));
    }
    push_module_arg(&mut args, &raw[start..])?;
    Ok(args)
}

fn push_module_arg(args: &mut Vec<String>, raw: &str) -> Result<()> {
    let raw = raw.trim();
    if !raw.is_empty() {
        args.push(strip_identifier(raw)?.to_string());
    }
    Ok(())
}

fn create_virtual_table(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: ObjectName,
    if_not_exists: bool,
    module_name: String,
    module_args: Vec<String>,
) -> Result<()> {
    let name = object_name(&name)?;
    create_virtual_table_named(session, db, name, if_not_exists, module_name, module_args)
}

fn create_virtual_table_named(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: String,
    if_not_exists: bool,
    module_name: String,
    module_args: Vec<String>,
) -> Result<()> {
    if db.table_id(&name).is_ok() || db.external_table(&name).is_some() {
        if if_not_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "table {name:?} already exists"
        )));
    }
    let module = module_name.to_ascii_lowercase();
    let descriptor = session.external_modules.descriptor(&module)?;
    let args = module_args
        .into_iter()
        .map(ModuleArg::Ident)
        .collect::<Vec<_>>();
    let entry = ExternalTableEntry::new(
        name.clone(),
        ExternalTableDefinition {
            module,
            args,
            declared_schema: descriptor.schema,
            hidden_columns: descriptor.hidden_columns,
            options: Default::default(),
            capabilities: descriptor.capabilities,
        },
        0,
    )
    .map_err(MongrelQueryError::Core)?;
    let entry = db.create_external_table(entry)?;
    let provider = session
        .external_modules
        .external_table_provider(db, &entry)?;
    session
        .ctx
        .register_table(&entry.name, provider)
        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
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
        Err(e)
            if matches!(e, mongreldb_core::MongrelError::NotFound(_))
                && db.external_table(name).is_some() =>
        {
            let entry = db
                .external_table(name)
                .ok_or_else(|| MongrelQueryError::Schema(format!("table {name:?} not found")))?;
            session
                .external_modules
                .destroy_external_table(db, &entry)?;
            db.drop_external_table(name)?;
            let _ = session.ctx.deregister_table(name);
            session.clear_cache();
            Ok(())
        }
        Err(e) if if_exists && matches!(e, mongreldb_core::MongrelError::NotFound(_)) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn drop_view(
    session: &MongrelSession,
    db: &Arc<Database>,
    name: &str,
    if_exists: bool,
) -> Result<()> {
    if session.view_definition(name).is_none() {
        if if_exists {
            return Ok(());
        }
        return Err(MongrelQueryError::Schema(format!(
            "view {name:?} does not exist"
        )));
    }
    session.drop_view(name);
    let trigger_names = db
        .triggers()
        .into_iter()
        .filter(|trigger| matches!(&trigger.target, TriggerTarget::View(target) if target == name))
        .map(|trigger| trigger.name)
        .collect::<Vec<_>>();
    for trigger in trigger_names {
        db.drop_trigger(&trigger)?;
    }
    session.clear_cache();
    Ok(())
}

fn external_table_write_error(op: &str, entry: &ExternalTableEntry) -> MongrelQueryError {
    let capability = if entry.capabilities.read_only {
        "read-only"
    } else if entry.capabilities.insert_only && op != "INSERT" {
        "insert-only"
    } else if !entry.capabilities.writable {
        "not writable"
    } else {
        "not wired to the SQL DML path yet"
    };
    MongrelQueryError::Schema(format!(
        "{op} is not supported for external table {:?} using module {:?} ({capability})",
        entry.name, entry.module
    ))
}

fn ensure_external_write_allowed(op: &str, entry: &ExternalTableEntry) -> Result<()> {
    let allowed = match op {
        "INSERT" => {
            !entry.capabilities.read_only
                && (entry.capabilities.writable || entry.capabilities.insert_only)
        }
        "UPDATE" | "DELETE" => !entry.capabilities.read_only && entry.capabilities.writable,
        _ => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(external_table_write_error(op, entry))
    }
}

fn refresh_external_table_provider(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
) -> Result<()> {
    let _ = session.ctx.deregister_table(&entry.name);
    let provider = session
        .external_modules
        .external_table_provider(db, entry)?;
    session
        .ctx
        .register_table(&entry.name, provider)
        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
    session.clear_cache();
    Ok(())
}

fn current_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
) -> Result<Vec<HashMap<u16, Value>>> {
    if let Some(staged) = session.sql_txn.lock().as_ref() {
        for op in staged.iter().rev() {
            if let PendingSqlOp::ExternalState { table, state, .. } = op {
                if table == &entry.name {
                    return session
                        .external_modules
                        .external_table_rows_from_state(entry, state);
                }
            }
        }
    }
    session.external_modules.external_table_rows(db, entry)
}

fn current_external_state(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
) -> Result<Vec<u8>> {
    if let Some(staged) = session.sql_txn.lock().as_ref() {
        for op in staged.iter().rev() {
            if let PendingSqlOp::ExternalState { table, state, .. } = op {
                if table == &entry.name {
                    return Ok(state.clone());
                }
            }
        }
    }
    crate::external_modules::external_table_state_bytes(db, entry)
}

fn stage_external_write(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    op: ExternalWriteOp,
) -> Result<()> {
    let base_state = current_external_state(session, db, entry)?;
    let (state, result, base_writes) = session
        .external_modules
        .external_table_write(db, entry, base_state, op)?;
    let mut ops = base_writes
        .into_iter()
        .map(pending_op_from_external_base_write)
        .collect::<Vec<_>>();
    ops.push(PendingSqlOp::ExternalState {
        table: entry.name.clone(),
        state,
        changes: result.changes,
    });
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

fn pending_op_from_external_base_write(op: ExternalBaseWrite) -> PendingSqlOp {
    match op {
        ExternalBaseWrite::Put { table, cells } => PendingSqlOp::Put { table, cells },
        ExternalBaseWrite::Delete { table, row_id } => PendingSqlOp::Delete {
            table,
            row_id: RowId(row_id),
        },
    }
}

fn insert_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    insert: Insert,
) -> Result<()> {
    ensure_external_write_allowed("INSERT", entry)?;
    if insert.on.is_some() {
        return Err(MongrelQueryError::Schema(
            "INSERT conflict actions are not supported for external tables".into(),
        ));
    }
    let schema = &entry.declared_schema;
    let columns = insert_columns(schema, &insert.columns)?;
    let value_rows = values_rows(insert.source.as_deref())?;
    let mut rows = Vec::with_capacity(value_rows.len());
    let mut inserted = 0_u64;
    for value_row in value_rows {
        if value_row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "INSERT has {} values for {} columns",
                value_row.len(),
                columns.len()
            )));
        }
        let mut row = HashMap::new();
        for (column, expr) in columns.iter().zip(value_row.iter()) {
            row.insert(column.id, expr_to_value(expr, column.ty)?);
        }
        rows.push(row);
        inserted = inserted.saturating_add(1);
    }
    let _ = inserted;
    stage_external_write(session, db, entry, ExternalWriteOp::Insert { rows })
}

fn update_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    update: sqlparser::ast::Update,
) -> Result<()> {
    ensure_external_write_allowed("UPDATE", entry)?;
    let schema = &entry.declared_schema;
    let mut rows = current_external_rows(session, db, entry)?;
    let mut changed = 0_u64;
    for row in &mut rows {
        let matches = match update.selection.as_ref() {
            Some(selection) => eval_bool_expr(selection, schema, row)?,
            None => true,
        };
        if matches {
            for assignment in &update.assignments {
                apply_assignment(schema, row, assignment, None)?;
            }
            changed = changed.saturating_add(1);
        }
    }
    stage_external_write(
        session,
        db,
        entry,
        ExternalWriteOp::ReplaceRows {
            rows,
            changes: changed,
        },
    )
}

fn delete_external_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    entry: &ExternalTableEntry,
    delete: Delete,
) -> Result<()> {
    ensure_external_write_allowed("DELETE", entry)?;
    let schema = &entry.declared_schema;
    let rows = current_external_rows(session, db, entry)?;
    let mut kept = Vec::with_capacity(rows.len());
    let mut deleted = 0_u64;
    for row in rows {
        if view_row_matches(delete.selection.as_ref(), schema, &row)? {
            deleted = deleted.saturating_add(1);
        } else {
            kept.push(row);
        }
    }
    stage_external_write(
        session,
        db,
        entry,
        ExternalWriteOp::ReplaceRows {
            rows: kept,
            changes: deleted,
        },
    )
}

fn create_trigger(
    session: &MongrelSession,
    db: &Arc<Database>,
    create: CreateTrigger,
) -> Result<()> {
    let or_replace = create.or_replace || create.or_alter;
    let trigger = trigger_from_sql(session, db, create)?;
    if or_replace {
        db.create_or_replace_trigger(trigger)?;
    } else {
        db.create_trigger(trigger)?;
    }
    Ok(())
}

fn create_trigger_if_not_exists_manual(
    session: &MongrelSession,
    db: &Arc<Database>,
    sql: &str,
) -> Result<()> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let rest = &sql["create trigger if not exists ".len()..];
    let rewritten = format!("create trigger {rest}");
    let statements = Parser::parse_sql(&GenericDialect {}, &rewritten)
        .map_err(|e| MongrelQueryError::Schema(format!("SQL parse error: {e}")))?;
    if statements.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "only one statement may be executed at a time".into(),
        ));
    }
    let Statement::CreateTrigger(trigger) = statements.into_iter().next().unwrap() else {
        return Err(MongrelQueryError::Schema(
            "expected CREATE TRIGGER IF NOT EXISTS".into(),
        ));
    };
    let name = object_name(&trigger.name)?;
    if db.trigger(&name).is_some() {
        return Ok(());
    }
    create_trigger(session, db, trigger)?;
    session.clear_cache();
    Ok(())
}

fn drop_trigger(db: &Arc<Database>, drop: DropTrigger) -> Result<()> {
    if drop.table_name.is_some() {
        return Err(MongrelQueryError::Schema(
            "DROP TRIGGER ON <table> is not required; trigger names are database-scoped".into(),
        ));
    }
    let name = object_name(&drop.trigger_name)?;
    match db.drop_trigger(&name) {
        Ok(()) => Ok(()),
        Err(e) if drop.if_exists && matches!(e, mongreldb_core::MongrelError::NotFound(_)) => {
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

fn trigger_from_sql(
    session: &MongrelSession,
    db: &Arc<Database>,
    create: CreateTrigger,
) -> Result<StoredTrigger> {
    if create.temporary || create.is_constraint || create.referenced_table_name.is_some() {
        return Err(MongrelQueryError::Schema(
            "TEMP/CONSTRAINT/FROM triggers are not supported".into(),
        ));
    }
    if !create.referencing.is_empty() || create.characteristics.is_some() {
        return Err(MongrelQueryError::Schema(
            "REFERENCING and trigger characteristics are not supported".into(),
        ));
    }
    if create.exec_body.is_some() {
        return Err(MongrelQueryError::Schema(
            "EXECUTE FUNCTION/PROCEDURE trigger bodies are not supported; use BEGIN ... END statements".into(),
        ));
    }
    match create.trigger_object {
        None | Some(TriggerObjectKind::ForEach(TriggerObject::Row)) => {}
        Some(_) => {
            return Err(MongrelQueryError::Schema(
                "only row-level triggers are supported".into(),
            ));
        }
    }

    let timing = match create.period.unwrap_or(TriggerPeriod::After) {
        TriggerPeriod::After => TriggerTiming::After,
        TriggerPeriod::Before => TriggerTiming::Before,
        TriggerPeriod::InsteadOf => TriggerTiming::InsteadOf,
        TriggerPeriod::For => {
            return Err(MongrelQueryError::Schema(
                "FOR is not a trigger timing".into(),
            ));
        }
    };
    if create.events.len() != 1 {
        return Err(MongrelQueryError::Schema(
            "triggers currently support exactly one event".into(),
        ));
    }
    let (event, update_of) = match &create.events[0] {
        SqlTriggerEvent::Insert => (TriggerEvent::Insert, Vec::new()),
        SqlTriggerEvent::Delete => (TriggerEvent::Delete, Vec::new()),
        SqlTriggerEvent::Update(columns) => (
            TriggerEvent::Update,
            columns.iter().map(|c| c.value.clone()).collect(),
        ),
        SqlTriggerEvent::Truncate => {
            return Err(MongrelQueryError::Schema(
                "TRUNCATE triggers are not supported".into(),
            ));
        }
    };
    let name = object_name(&create.name)?;
    let target_name = object_name(&create.table_name)?;
    let (target, target_schema, target_columns) = match timing {
        TriggerTiming::Before | TriggerTiming::After => {
            if session.view_schema(&target_name).is_some() {
                return Err(MongrelQueryError::Schema(
                    "views support INSTEAD OF triggers, not BEFORE/AFTER triggers".into(),
                ));
            }
            (
                TriggerTarget::Table(target_name.clone()),
                table_schema(db, &target_name)?,
                Vec::new(),
            )
        }
        TriggerTiming::InsteadOf => {
            let schema = session.view_schema(&target_name).ok_or_else(|| {
                MongrelQueryError::Schema(format!(
                    "INSTEAD OF trigger target view {target_name:?} does not exist"
                ))
            })?;
            if schema.columns.is_empty() {
                return Err(MongrelQueryError::Schema(
                    "INSTEAD OF triggers require CREATE VIEW column names".into(),
                ));
            }
            (
                TriggerTarget::View(target_name.clone()),
                schema.clone(),
                schema.columns.clone(),
            )
        }
    };
    let when = create
        .condition
        .as_ref()
        .map(|expr| trigger_expr_from_sql(expr, &target_schema, event))
        .transpose()?;
    let statements = create.statements.ok_or_else(|| {
        MongrelQueryError::Schema("trigger body must contain BEGIN ... END statements".into())
    })?;
    let mut steps = Vec::new();
    for statement in trigger_statement_list(&statements) {
        steps.extend(trigger_steps_from_statement(
            db,
            statement,
            &target_schema,
            event,
        )?);
    }
    let trigger = StoredTrigger::new(
        name,
        TriggerDefinition {
            target,
            timing,
            event,
            update_of,
            target_columns,
            when,
            program: TriggerProgram { steps },
        },
        0,
    )
    .map_err(MongrelQueryError::Core)?;
    Ok(trigger)
}

fn trigger_statement_list(statements: &ConditionalStatements) -> &[Statement] {
    match statements {
        ConditionalStatements::Sequence { statements } => statements,
        ConditionalStatements::BeginEnd(block) => &block.statements,
    }
}

fn trigger_steps_from_statement(
    db: &Arc<Database>,
    statement: &Statement,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    match statement {
        Statement::Insert(insert) => trigger_insert_steps(db, insert, trigger_schema, event),
        Statement::Update(update) => trigger_update_step(db, update, trigger_schema, event),
        Statement::Delete(delete) => trigger_delete_step(db, delete, trigger_schema, event),
        Statement::Query(query) => trigger_query_step(query, trigger_schema, event),
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported trigger body statement: {other}"
        ))),
    }
}

fn trigger_insert_steps(
    db: &Arc<Database>,
    insert: &Insert,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    if insert.returning.is_some() || insert.on.is_some() {
        return Err(MongrelQueryError::Schema(
            "trigger INSERT does not support RETURNING or conflict actions".into(),
        ));
    }
    let table = match &insert.table {
        TableObject::TableName(name) => object_name(name)?,
        _ => {
            return Err(MongrelQueryError::Schema(
                "trigger INSERT target must be a table name".into(),
            ));
        }
    };
    let schema = table_schema(db, &table)?;
    let columns = insert_columns(&schema, &insert.columns)?;
    let rows = values_rows(insert.source.as_deref())?;
    let mut steps = Vec::with_capacity(rows.len());
    for row in rows {
        if row.len() != columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "trigger INSERT has {} values for {} columns",
                row.len(),
                columns.len()
            )));
        }
        let mut cells = Vec::with_capacity(row.len());
        for (col, expr) in columns.iter().zip(row.iter()) {
            cells.push(TriggerCell {
                column_id: col.id,
                value: trigger_value_from_sql(expr, trigger_schema, event, Some(col.ty))?,
            });
        }
        steps.push(TriggerStep::Insert {
            table: table.clone(),
            cells,
        });
    }
    Ok(steps)
}

fn trigger_update_step(
    db: &Arc<Database>,
    update: &sqlparser::ast::Update,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    if update.returning.is_some() || update.output.is_some() || update.from.is_some() {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE does not support RETURNING/OUTPUT/FROM".into(),
        ));
    }
    if !update.table.joins.is_empty() {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE with joins is not supported".into(),
        ));
    }
    let table = table_factor_name(&update.table.relation)?;
    let schema = table_schema(db, &table)?;
    let pk = trigger_pk_condition(update.selection.as_ref(), &schema, trigger_schema, event)?;
    let mut cells = Vec::with_capacity(update.assignments.len());
    for assignment in &update.assignments {
        let column_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name(name)?,
            AssignmentTarget::Tuple(_) => {
                return Err(MongrelQueryError::Schema(
                    "trigger UPDATE tuple assignments are not supported".into(),
                ));
            }
        };
        let col = schema
            .column(&column_name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
        cells.push(TriggerCell {
            column_id: col.id,
            value: trigger_value_from_sql(&assignment.value, trigger_schema, event, Some(col.ty))?,
        });
    }
    Ok(vec![TriggerStep::UpdateByPk { table, pk, cells }])
}

fn trigger_delete_step(
    db: &Arc<Database>,
    delete: &Delete,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    let table = single_from_table(&delete.from)?;
    let schema = table_schema(db, &table)?;
    let pk = trigger_pk_condition(delete.selection.as_ref(), &schema, trigger_schema, event)?;
    Ok(vec![TriggerStep::DeleteByPk { table, pk }])
}

fn trigger_query_step(
    query: &Query,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<Vec<TriggerStep>> {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only simple SELECT".into(),
        ));
    };
    if select.projection.len() != 1 || !select.from.is_empty() {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only SELECT RAISE(...)".into(),
        ));
    }
    let sqlparser::ast::SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0]
    else {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only SELECT RAISE(...)".into(),
        ));
    };
    if !object_name(&func.name)?.eq_ignore_ascii_case("raise") {
        return Err(MongrelQueryError::Schema(
            "trigger SELECT body supports only SELECT RAISE(...)".into(),
        ));
    }
    let FunctionArguments::List(args) = &func.args else {
        return Err(MongrelQueryError::Schema(
            "RAISE requires argument list".into(),
        ));
    };
    if args.args.is_empty() {
        return Err(MongrelQueryError::Schema("RAISE requires an action".into()));
    }
    let action = raise_action_from_arg(&args.args[0])?;
    let message = if action == TriggerRaiseAction::Ignore {
        TriggerValue::Literal(Value::Null)
    } else {
        let Some(arg) = args.args.get(1) else {
            return Err(MongrelQueryError::Schema(
                "RAISE action requires a message".into(),
            ));
        };
        trigger_value_from_sql(
            function_arg_expr(arg)?,
            trigger_schema,
            event,
            Some(TypeId::Bytes),
        )?
    };
    Ok(vec![TriggerStep::Raise { action, message }])
}

fn raise_action_from_arg(arg: &FunctionArg) -> Result<TriggerRaiseAction> {
    let expr = function_arg_expr(arg)?;
    let name = match expr {
        Expr::Identifier(ident) => ident.value.to_ascii_lowercase(),
        Expr::Value(v) => match sql_value_to_value(&v.value, Some(TypeId::Bytes))? {
            Value::Bytes(bytes) => String::from_utf8_lossy(&bytes).to_ascii_lowercase(),
            _ => {
                return Err(MongrelQueryError::Schema(
                    "RAISE action must be an identifier or string".into(),
                ));
            }
        },
        _ => {
            return Err(MongrelQueryError::Schema(
                "RAISE action must be an identifier or string".into(),
            ));
        }
    };
    match name.as_str() {
        "abort" => Ok(TriggerRaiseAction::Abort),
        "fail" => Ok(TriggerRaiseAction::Fail),
        "rollback" => Ok(TriggerRaiseAction::Rollback),
        "ignore" => Ok(TriggerRaiseAction::Ignore),
        _ => Err(MongrelQueryError::Schema(format!(
            "unsupported RAISE action {name:?}"
        ))),
    }
}

fn function_arg_expr(arg: &FunctionArg) -> Result<&Expr> {
    match arg {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        _ => Err(MongrelQueryError::Schema(
            "trigger function arguments must be positional expressions".into(),
        )),
    }
}

fn trigger_pk_condition(
    selection: Option<&Expr>,
    step_schema: &CoreSchema,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<TriggerValue> {
    let pk = step_schema.primary_key().ok_or_else(|| {
        MongrelQueryError::Schema("trigger UPDATE/DELETE target needs a primary key".into())
    })?;
    let Some(Expr::BinaryOp { left, op, right }) = selection else {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE/DELETE requires WHERE pk = expr".into(),
        ));
    };
    if *op != BinaryOperator::Eq {
        return Err(MongrelQueryError::Schema(
            "trigger UPDATE/DELETE requires WHERE pk = expr".into(),
        ));
    }
    if expr_is_column(left, &pk.name) {
        trigger_value_from_sql(right, trigger_schema, event, Some(pk.ty))
    } else if expr_is_column(right, &pk.name) {
        trigger_value_from_sql(left, trigger_schema, event, Some(pk.ty))
    } else {
        Err(MongrelQueryError::Schema(format!(
            "trigger UPDATE/DELETE WHERE must compare primary key {}",
            pk.name
        )))
    }
}

fn expr_is_column(expr: &Expr, column: &str) -> bool {
    match expr {
        Expr::Identifier(ident) => ident.value == column,
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => parts[1].value == column,
        _ => false,
    }
}

fn trigger_expr_from_sql(
    expr: &Expr,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
) -> Result<TriggerExpr> {
    match expr {
        Expr::Nested(inner) => trigger_expr_from_sql(inner, trigger_schema, event),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::Eq => Ok(TriggerExpr::Eq {
                left: trigger_value_from_sql(left, trigger_schema, event, None)?,
                right: trigger_value_from_sql(right, trigger_schema, event, None)?,
            }),
            BinaryOperator::NotEq => Ok(TriggerExpr::NotEq {
                left: trigger_value_from_sql(left, trigger_schema, event, None)?,
                right: trigger_value_from_sql(right, trigger_schema, event, None)?,
            }),
            _ => Err(MongrelQueryError::Schema(format!(
                "unsupported trigger WHEN operator {op}"
            ))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => invert_trigger_expr(trigger_expr_from_sql(expr, trigger_schema, event)?),
        Expr::IsNull(inner) => Ok(TriggerExpr::IsNull(trigger_value_from_sql(
            inner,
            trigger_schema,
            event,
            None,
        )?)),
        Expr::IsNotNull(inner) => Ok(TriggerExpr::IsNotNull(trigger_value_from_sql(
            inner,
            trigger_schema,
            event,
            None,
        )?)),
        _ => Ok(TriggerExpr::Value(trigger_value_from_sql(
            expr,
            trigger_schema,
            event,
            Some(TypeId::Bool),
        )?)),
    }
}

fn invert_trigger_expr(expr: TriggerExpr) -> Result<TriggerExpr> {
    match expr {
        TriggerExpr::Eq { left, right } => Ok(TriggerExpr::NotEq { left, right }),
        TriggerExpr::NotEq { left, right } => Ok(TriggerExpr::Eq { left, right }),
        TriggerExpr::Lt { left, right } => Ok(TriggerExpr::Gte { left, right }),
        TriggerExpr::Lte { left, right } => Ok(TriggerExpr::Gt { left, right }),
        TriggerExpr::Gt { left, right } => Ok(TriggerExpr::Lte { left, right }),
        TriggerExpr::Gte { left, right } => Ok(TriggerExpr::Lt { left, right }),
        TriggerExpr::IsNull(value) => Ok(TriggerExpr::IsNotNull(value)),
        TriggerExpr::IsNotNull(value) => Ok(TriggerExpr::IsNull(value)),
        TriggerExpr::And { left, right } => Ok(TriggerExpr::Or {
            left: Box::new(invert_trigger_expr(*left)?),
            right: Box::new(invert_trigger_expr(*right)?),
        }),
        TriggerExpr::Or { left, right } => Ok(TriggerExpr::And {
            left: Box::new(invert_trigger_expr(*left)?),
            right: Box::new(invert_trigger_expr(*right)?),
        }),
        TriggerExpr::Not(inner) => Ok(*inner),
        TriggerExpr::Value(TriggerValue::Literal(Value::Bool(value))) => Ok(TriggerExpr::Value(
            TriggerValue::Literal(Value::Bool(!value)),
        )),
        TriggerExpr::Value(value) => Err(MongrelQueryError::Schema(format!(
            "trigger WHEN NOT only supports comparisons, IS NULL, IS NOT NULL, or boolean literals; got {value:?}"
        ))),
    }
}

fn trigger_value_from_sql(
    expr: &Expr,
    trigger_schema: &CoreSchema,
    event: TriggerEvent,
    literal_type: Option<TypeId>,
) -> Result<TriggerValue> {
    match expr {
        Expr::Nested(inner) => trigger_value_from_sql(inner, trigger_schema, event, literal_type),
        Expr::Value(v) => {
            let value = sql_value_to_value(&v.value, literal_type)?;
            let value = match literal_type {
                Some(ty) => coerce_value(value, ty)?,
                None => value,
            };
            Ok(TriggerValue::Literal(value))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match trigger_value_from_sql(expr, trigger_schema, event, literal_type)? {
            TriggerValue::Literal(Value::Int64(v)) => {
                Ok(TriggerValue::Literal(Value::Int64(v.saturating_neg())))
            }
            TriggerValue::Literal(Value::Float64(v)) => {
                Ok(TriggerValue::Literal(Value::Float64(-v)))
            }
            _ => Err(MongrelQueryError::Schema(
                "trigger unary minus requires a numeric literal".into(),
            )),
        },
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let row = parts[0].value.to_ascii_lowercase();
            let column = trigger_schema.column(&parts[1].value).ok_or_else(|| {
                MongrelQueryError::Schema(format!("unknown trigger column {}", parts[1].value))
            })?;
            match row.as_str() {
                "new" => {
                    if event == TriggerEvent::Delete {
                        return Err(MongrelQueryError::Schema(
                            "DELETE triggers cannot reference NEW".into(),
                        ));
                    }
                    Ok(TriggerValue::NewColumn(column.id))
                }
                "old" => {
                    if event == TriggerEvent::Insert {
                        return Err(MongrelQueryError::Schema(
                            "INSERT triggers cannot reference OLD".into(),
                        ));
                    }
                    Ok(TriggerValue::OldColumn(column.id))
                }
                _ => Err(MongrelQueryError::Schema(format!(
                    "unsupported trigger row qualifier {row:?}"
                ))),
            }
        }
        other => Err(MongrelQueryError::Schema(format!(
            "unsupported trigger value expression {other}"
        ))),
    }
}

fn create_view(session: &MongrelSession, view: CreateView) -> Result<()> {
    if view.materialized {
        return Err(MongrelQueryError::Schema(
            "CREATE MATERIALIZED VIEW is not supported; use CREATE VIEW".into(),
        ));
    }
    let name = object_name(&view.name)?;
    let (schema, input_types) = view_schema_from_columns(&view.columns)?;
    session.create_view_with_schema(&name, &view.query.to_string(), schema, input_types);
    Ok(())
}

fn view_schema_from_columns(
    columns: &[sqlparser::ast::ViewColumnDef],
) -> Result<(CoreSchema, HashMap<u16, Option<TypeId>>)> {
    let mut seen = HashSet::new();
    let mut out = Vec::with_capacity(columns.len());
    let mut input_types = HashMap::new();
    for (idx, column) in columns.iter().enumerate() {
        let name = column.name.value.clone();
        if !seen.insert(name.clone()) {
            return Err(MongrelQueryError::Schema(format!(
                "duplicate view column {name}"
            )));
        }
        let id = u16::try_from(idx + 1).map_err(|_| {
            MongrelQueryError::Schema("view has too many columns for trigger routing".into())
        })?;
        let ty = match &column.data_type {
            Some(data_type) => sql_type_to_core(data_type)?,
            None => TypeId::Bytes,
        };
        input_types.insert(
            id,
            column
                .data_type
                .as_ref()
                .map(sql_type_to_core)
                .transpose()?,
        );
        out.push(CoreColumnDef {
            id,
            name,
            ty,
            flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
        });
    }
    Ok((
        CoreSchema {
            columns: out,
            ..CoreSchema::default()
        },
        input_types,
    ))
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
    let table = match &insert.table {
        TableObject::TableName(name) => object_name(name)?,
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
    if session.view_definition(&table).is_some() {
        return insert_view_rows(session, db, &table, insert);
    }
    if let Some(entry) = db.external_table(&table) {
        return insert_external_rows(session, db, &entry, insert);
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
    let changes = logical_changes(&ops);
    let last_insert_rowid = last_insert_pk(&ops, &schema);
    stage_or_apply(session, db, ops, changes, last_insert_rowid)
}

#[derive(Clone)]
struct SqlTriggerEventImage {
    kind: TriggerEvent,
    old: Option<HashMap<u16, Value>>,
    new: Option<HashMap<u16, Value>>,
}

fn insert_view_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    view: &str,
    insert: Insert,
) -> Result<()> {
    if insert.on.is_some() {
        return Err(MongrelQueryError::Schema(
            "INSERT conflict actions are not supported for views".into(),
        ));
    }
    let view_def = session
        .view_definition(view)
        .ok_or_else(|| MongrelQueryError::Schema(format!("view {view:?} does not exist")))?;
    if view_def.schema.columns.is_empty() {
        return Err(MongrelQueryError::Schema(
            "INSERT into a view requires CREATE VIEW column names".into(),
        ));
    }
    let triggers = instead_of_triggers(db, view, TriggerEvent::Insert, None);
    if triggers.is_empty() {
        return Err(MongrelQueryError::Schema(format!(
            "cannot INSERT into view {view:?} without an INSTEAD OF INSERT trigger"
        )));
    }

    let columns = insert_columns(&view_def.schema, &insert.columns)?;
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
        let mut new = HashMap::new();
        for (col, expr) in columns.iter().zip(row.iter()) {
            let mut value = expr_to_untyped_value(expr)?;
            if let Some(ty) = view_def.input_types.get(&col.id).and_then(|ty| *ty) {
                value = coerce_value(value, ty)?;
            }
            new.insert(col.id, value);
        }
        let event = SqlTriggerEventImage {
            kind: TriggerEvent::Insert,
            old: None,
            new: Some(new),
        };
        for trigger in &triggers {
            if execute_instead_of_trigger_program(db, trigger, &event, &mut ops)?
                == SqlTriggerProgramOutcome::Ignore
            {
                break;
            }
        }
    }
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

fn instead_of_triggers(
    db: &Arc<Database>,
    view: &str,
    event: TriggerEvent,
    changed_columns: Option<&[u16]>,
) -> Vec<StoredTrigger> {
    db.triggers()
        .into_iter()
        .filter(|trigger| {
            if !trigger.enabled
                || trigger.timing != TriggerTiming::InsteadOf
                || trigger.event != event
                || !matches!(&trigger.target, TriggerTarget::View(target) if target == view)
            {
                return false;
            }
            if event == TriggerEvent::Update && !trigger.update_of.is_empty() {
                let Some(changed_columns) = changed_columns else {
                    return false;
                };
                return trigger.update_of.iter().any(|name| {
                    trigger
                        .target_columns
                        .iter()
                        .find(|column| column.name == *name)
                        .is_some_and(|column| changed_columns.contains(&column.id))
                });
            }
            true
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqlTriggerProgramOutcome {
    Continue,
    Ignore,
}

fn execute_instead_of_trigger_program(
    db: &Arc<Database>,
    trigger: &StoredTrigger,
    event: &SqlTriggerEventImage,
    ops: &mut Vec<PendingSqlOp>,
) -> Result<SqlTriggerProgramOutcome> {
    if let Some(when) = &trigger.when {
        if !eval_instead_of_trigger_expr(when, event)? {
            return Ok(SqlTriggerProgramOutcome::Continue);
        }
    }
    for step in &trigger.program.steps {
        match step {
            TriggerStep::SetNew { .. } => {
                return Err(MongrelQueryError::Schema(
                    "SET NEW is not valid in INSTEAD OF triggers".into(),
                ));
            }
            TriggerStep::Insert { table, cells } => {
                ops.push(PendingSqlOp::Put {
                    table: table.clone(),
                    cells: eval_instead_of_trigger_cells(cells, event)?,
                });
            }
            TriggerStep::UpdateByPk { table, pk, cells } => {
                let pk = eval_instead_of_trigger_value(pk, event)?;
                let handle = db.table(table)?;
                let row_id = handle.lock().lookup_pk(&pk.encode_key()).ok_or_else(|| {
                    MongrelQueryError::Schema(format!(
                        "trigger {:?} update target not found",
                        trigger.name
                    ))
                })?;
                let old = {
                    let guard = handle.lock();
                    guard.get(row_id, guard.snapshot()).ok_or_else(|| {
                        MongrelQueryError::Schema(format!(
                            "trigger {:?} update target not visible",
                            trigger.name
                        ))
                    })?
                };
                let mut merged = old.columns;
                for (column_id, value) in eval_instead_of_trigger_cells(cells, event)? {
                    merged.insert(column_id, value);
                }
                ops.push(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id,
                });
                ops.push(PendingSqlOp::Put {
                    table: table.clone(),
                    cells: map_to_cells(&merged),
                });
            }
            TriggerStep::DeleteByPk { table, pk } => {
                let pk = eval_instead_of_trigger_value(pk, event)?;
                let row_id = db
                    .table(table)?
                    .lock()
                    .lookup_pk(&pk.encode_key())
                    .ok_or_else(|| {
                        MongrelQueryError::Schema(format!(
                            "trigger {:?} delete target not found",
                            trigger.name
                        ))
                    })?;
                ops.push(PendingSqlOp::Delete {
                    table: table.clone(),
                    row_id,
                });
            }
            TriggerStep::Select { .. } => {}
            TriggerStep::Foreach { .. }
            | TriggerStep::DeleteWhere { .. }
            | TriggerStep::UpdateWhere { .. } => {
                return Err(MongrelQueryError::Schema(
                    "FOREACH/DELETE WHERE/UPDATE WHERE are not valid in INSTEAD OF triggers".into(),
                ));
            }
            TriggerStep::Raise { action, message } => match action {
                TriggerRaiseAction::Ignore => return Ok(SqlTriggerProgramOutcome::Ignore),
                TriggerRaiseAction::Abort
                | TriggerRaiseAction::Fail
                | TriggerRaiseAction::Rollback => {
                    let message = eval_instead_of_trigger_value(message, event)?;
                    return Err(MongrelQueryError::Schema(format!(
                        "trigger {:?} raised: {}",
                        trigger.name,
                        trigger_message(message)
                    )));
                }
            },
        }
    }
    Ok(SqlTriggerProgramOutcome::Continue)
}

fn eval_instead_of_trigger_cells(
    cells: &[TriggerCell],
    event: &SqlTriggerEventImage,
) -> Result<Vec<(u16, Value)>> {
    cells
        .iter()
        .map(|cell| {
            Ok((
                cell.column_id,
                eval_instead_of_trigger_value(&cell.value, event)?,
            ))
        })
        .collect()
}

fn eval_instead_of_trigger_expr(expr: &TriggerExpr, event: &SqlTriggerEventImage) -> Result<bool> {
    match expr {
        TriggerExpr::Value(value) => match eval_instead_of_trigger_value(value, event)? {
            Value::Bool(value) => Ok(value),
            Value::Null => Ok(false),
            other => Err(MongrelQueryError::Schema(format!(
                "trigger WHEN value must be boolean, got {other:?}"
            ))),
        },
        TriggerExpr::Eq { left, right } => Ok(trigger_values_equal(
            &eval_instead_of_trigger_value(left, event)?,
            &eval_instead_of_trigger_value(right, event)?,
        )),
        TriggerExpr::NotEq { left, right } => Ok(!trigger_values_equal(
            &eval_instead_of_trigger_value(left, event)?,
            &eval_instead_of_trigger_value(right, event)?,
        )),
        TriggerExpr::IsNull(value) => Ok(matches!(
            eval_instead_of_trigger_value(value, event)?,
            Value::Null
        )),
        TriggerExpr::IsNotNull(value) => Ok(!matches!(
            eval_instead_of_trigger_value(value, event)?,
            Value::Null
        )),
        TriggerExpr::Lt { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::Lt,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::Lte { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::LtEq,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::Gt { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::Gt,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::Gte { left, right } => Ok(compare_values(
            &eval_instead_of_trigger_value(left, event)?,
            &BinaryOperator::GtEq,
            &eval_instead_of_trigger_value(right, event)?,
        )?),
        TriggerExpr::And { left, right } => Ok(eval_instead_of_trigger_expr(left, event)?
            && eval_instead_of_trigger_expr(right, event)?),
        TriggerExpr::Or { left, right } => Ok(eval_instead_of_trigger_expr(left, event)?
            || eval_instead_of_trigger_expr(right, event)?),
        TriggerExpr::Not(inner) => Ok(!eval_instead_of_trigger_expr(inner, event)?),
    }
}

fn eval_instead_of_trigger_value(
    value: &TriggerValue,
    event: &SqlTriggerEventImage,
) -> Result<Value> {
    match value {
        TriggerValue::Literal(value) => Ok(value.clone()),
        TriggerValue::NewColumn(column_id) => {
            if event.kind == TriggerEvent::Delete {
                return Err(MongrelQueryError::Schema(
                    "DELETE triggers cannot reference NEW".into(),
                ));
            }
            event
                .new
                .as_ref()
                .and_then(|row| row.get(column_id))
                .cloned()
                .ok_or_else(|| MongrelQueryError::Schema("NEW column is not available".into()))
        }
        TriggerValue::OldColumn(column_id) => {
            if event.kind == TriggerEvent::Insert {
                return Err(MongrelQueryError::Schema(
                    "INSERT triggers cannot reference OLD".into(),
                ));
            }
            event
                .old
                .as_ref()
                .and_then(|row| row.get(column_id))
                .cloned()
                .ok_or_else(|| MongrelQueryError::Schema("OLD column is not available".into()))
        }
        TriggerValue::SelectedColumn(_) => Err(MongrelQueryError::Schema(
            "SELECTED column is not available in INSTEAD OF triggers".into(),
        )),
    }
}

/// Extract a `usize` from a SQL `Expr` (e.g. `Expr::Value(Number("5", false))`).
fn expr_to_usize(expr: &sqlparser::ast::Expr) -> Option<usize> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    match expr {
        Expr::Value(v) => match &v.value {
            SqlValue::Number(s, _) => s.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

/// Sort rows by ORDER BY expressions. Each `OrderByExpr` maps to a column
/// name; we resolve the name to a column id via the schema, then compare
/// the encoded key bytes for ordering.
fn apply_order_by(
    rows: &mut [mongreldb_core::memtable::Row],
    order_by: &[sqlparser::ast::OrderByExpr],
    schema: &mongreldb_core::schema::Schema,
) -> Result<()> {
    // Build a name → column_id lookup from the schema.
    let name_to_id: HashMap<String, u16> = schema
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.id))
        .collect();
    rows.sort_by(|a, b| {
        for expr in order_by {
            let col_name = match &expr.expr {
                sqlparser::ast::Expr::Identifier(ident) => &ident.value,
                sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                    &idents.last().unwrap().value
                }
                _ => continue,
            };
            let col_id = match name_to_id.get(col_name) {
                Some(id) => *id,
                None => continue,
            };
            let va = a.columns.get(&col_id);
            let vb = b.columns.get(&col_id);
            let ord = match (va, vb) {
                (Some(va), Some(vb)) => va.encode_key().cmp(&vb.encode_key()),
                _ => std::cmp::Ordering::Equal,
            };
            let ord = if expr.options.asc == Some(false) {
                ord.reverse()
            } else {
                ord
            };
            if ord != std::cmp::Ordering::Equal {
                return ord;
            }
        }
        std::cmp::Ordering::Equal
    });
    Ok(())
}

async fn update_rows(
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
    if session.view_definition(&table).is_some() {
        return update_view_rows(session, db, &table, update).await;
    }
    if let Some(entry) = db.external_table(&table) {
        return update_external_rows(session, db, &entry, update);
    }
    let (schema, rows) = visible_rows(db, &table)?;
    let mut matched: Vec<_> = rows
        .into_iter()
        .filter(|row| predicate_matches(update.selection.as_ref(), &schema, row).unwrap_or(false))
        .collect();
    // Apply ORDER BY + LIMIT if present.
    if !update.order_by.is_empty() {
        apply_order_by(&mut matched, &update.order_by, &schema)?;
    }
    if let Some(limit_expr) = &update.limit {
        if let Some(n) = expr_to_usize(limit_expr) {
            matched.truncate(n);
        }
    }
    let mut ops = Vec::new();
    for row in &matched {
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
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

async fn delete_rows(session: &MongrelSession, db: &Arc<Database>, delete: Delete) -> Result<()> {
    if delete.returning.is_some()
        || delete.output.is_some()
        || delete.using.is_some()
        || !delete.tables.is_empty()
    {
        return Err(MongrelQueryError::Schema(
            "DELETE USING/RETURNING and multi-table DELETE are not supported".into(),
        ));
    }
    let table = single_from_table(&delete.from)?;
    if session.view_definition(&table).is_some() {
        return delete_view_rows(session, db, &table, delete).await;
    }
    if let Some(entry) = db.external_table(&table) {
        return delete_external_rows(session, db, &entry, delete);
    }
    let (schema, rows) = visible_rows(db, &table)?;
    let mut matched: Vec<_> = rows
        .into_iter()
        .filter(|row| predicate_matches(delete.selection.as_ref(), &schema, row).unwrap_or(false))
        .collect();
    // Apply ORDER BY + LIMIT if present.
    if !delete.order_by.is_empty() {
        apply_order_by(&mut matched, &delete.order_by, &schema)?;
    }
    if let Some(limit_expr) = &delete.limit {
        if let Some(n) = expr_to_usize(limit_expr) {
            matched.truncate(n);
        }
    }
    let ops = matched
        .into_iter()
        .map(|row| PendingSqlOp::Delete {
            table: table.clone(),
            row_id: row.row_id,
        })
        .collect::<Vec<_>>();
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

async fn update_view_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    view: &str,
    update: sqlparser::ast::Update,
) -> Result<()> {
    let view_def = session
        .view_definition(view)
        .ok_or_else(|| MongrelQueryError::Schema(format!("view {view:?} does not exist")))?;
    let changed_columns = view_assignment_targets(&view_def.schema, &update.assignments)?;
    let triggers = instead_of_triggers(db, view, TriggerEvent::Update, Some(&changed_columns));
    if triggers.is_empty() {
        return Err(MongrelQueryError::Schema(format!(
            "cannot UPDATE view {view:?} without a matching INSTEAD OF UPDATE trigger"
        )));
    }

    let rows = materialize_view_rows(session, &view_def).await?;
    let mut ops = Vec::new();
    for old in rows {
        if !view_row_matches(update.selection.as_ref(), &view_def.schema, &old)? {
            continue;
        }
        let mut new = old.clone();
        for assignment in &update.assignments {
            apply_view_assignment(
                &view_def.schema,
                &view_def.input_types,
                &mut new,
                assignment,
            )?;
        }
        let event = SqlTriggerEventImage {
            kind: TriggerEvent::Update,
            old: Some(old),
            new: Some(new),
        };
        for trigger in &triggers {
            if execute_instead_of_trigger_program(db, trigger, &event, &mut ops)?
                == SqlTriggerProgramOutcome::Ignore
            {
                break;
            }
        }
    }
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

async fn delete_view_rows(
    session: &MongrelSession,
    db: &Arc<Database>,
    view: &str,
    delete: Delete,
) -> Result<()> {
    let view_def = session
        .view_definition(view)
        .ok_or_else(|| MongrelQueryError::Schema(format!("view {view:?} does not exist")))?;
    let triggers = instead_of_triggers(db, view, TriggerEvent::Delete, None);
    if triggers.is_empty() {
        return Err(MongrelQueryError::Schema(format!(
            "cannot DELETE from view {view:?} without an INSTEAD OF DELETE trigger"
        )));
    }

    let rows = materialize_view_rows(session, &view_def).await?;
    let mut ops = Vec::new();
    for old in rows {
        if !view_row_matches(delete.selection.as_ref(), &view_def.schema, &old)? {
            continue;
        }
        let event = SqlTriggerEventImage {
            kind: TriggerEvent::Delete,
            old: Some(old),
            new: None,
        };
        for trigger in &triggers {
            if execute_instead_of_trigger_program(db, trigger, &event, &mut ops)?
                == SqlTriggerProgramOutcome::Ignore
            {
                break;
            }
        }
    }
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

fn view_assignment_targets(schema: &CoreSchema, assignments: &[Assignment]) -> Result<Vec<u16>> {
    let mut out = Vec::with_capacity(assignments.len());
    for assignment in assignments {
        let column_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => object_name(name)?,
            AssignmentTarget::Tuple(_) => {
                return Err(MongrelQueryError::Schema(
                    "view UPDATE tuple assignments are not supported".into(),
                ));
            }
        };
        let column = schema
            .column(&column_name)
            .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
        out.push(column.id);
    }
    Ok(out)
}

fn apply_view_assignment(
    schema: &CoreSchema,
    input_types: &HashMap<u16, Option<TypeId>>,
    row: &mut HashMap<u16, Value>,
    assignment: &Assignment,
) -> Result<()> {
    let column_name = match &assignment.target {
        AssignmentTarget::ColumnName(name) => object_name(name)?,
        AssignmentTarget::Tuple(_) => {
            return Err(MongrelQueryError::Schema(
                "view UPDATE tuple assignments are not supported".into(),
            ));
        }
    };
    let column = schema
        .column(&column_name)
        .ok_or_else(|| MongrelQueryError::Schema(format!("unknown column {column_name}")))?;
    let mut value = eval_value_expr(&assignment.value, schema, row, None)?;
    if let Some(ty) = input_types.get(&column.id).and_then(|ty| *ty) {
        value = coerce_value(value, ty)?;
    }
    row.insert(column.id, value);
    Ok(())
}

fn view_row_matches(
    selection: Option<&Expr>,
    schema: &CoreSchema,
    row: &HashMap<u16, Value>,
) -> Result<bool> {
    match selection {
        Some(expr) => eval_bool_expr(expr, schema, row),
        None => Ok(true),
    }
}

async fn materialize_view_rows(
    session: &MongrelSession,
    view: &crate::ViewDef,
) -> Result<Vec<HashMap<u16, Value>>> {
    if view.schema.columns.is_empty() {
        return Err(MongrelQueryError::Schema(
            "view write routing requires CREATE VIEW column names".into(),
        ));
    }
    let sql = format!(
        "SELECT * FROM ({}) AS __mongreldb_view_materialize",
        view.sql
    );
    let sql = crate::rewrite_compat_function_calls(&session.resolve_view_sql(&sql));
    let df = session
        .ctx
        .sql(&sql)
        .await
        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
    let batches = df
        .collect()
        .await
        .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;

    let mut out = Vec::new();
    for batch in batches {
        if batch.num_columns() != view.schema.columns.len() {
            return Err(MongrelQueryError::Schema(format!(
                "view query returned {} columns for {} declared view columns",
                batch.num_columns(),
                view.schema.columns.len()
            )));
        }
        for row_idx in 0..batch.num_rows() {
            let mut view_row = HashMap::new();
            for (col_idx, view_col) in view.schema.columns.iter().enumerate() {
                let scalar = datafusion::common::ScalarValue::try_from_array(
                    batch.column(col_idx).as_ref(),
                    row_idx,
                )
                .map_err(|e| MongrelQueryError::DataFusion(e.to_string()))?;
                let mut value = scalar_to_core_value(scalar)?;
                if let Some(ty) = view.input_types.get(&view_col.id).and_then(|ty| *ty) {
                    value = coerce_value(value, ty)?;
                }
                view_row.insert(view_col.id, value);
            }
            out.push(view_row);
        }
    }
    Ok(out)
}

fn scalar_to_core_value(value: datafusion::common::ScalarValue) -> Result<Value> {
    use datafusion::common::ScalarValue;
    if value.is_null() {
        return Ok(Value::Null);
    }
    match value {
        ScalarValue::Boolean(Some(v)) => Ok(Value::Bool(v)),
        ScalarValue::Float16(Some(v)) => Ok(Value::Float64(f32::from(v) as f64)),
        ScalarValue::Float32(Some(v)) => Ok(Value::Float64(v as f64)),
        ScalarValue::Float64(Some(v)) => Ok(Value::Float64(v)),
        ScalarValue::Int8(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Int16(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Int32(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Int64(Some(v)) => Ok(Value::Int64(v)),
        ScalarValue::UInt8(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::UInt16(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::UInt32(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::UInt64(Some(v)) => i64::try_from(v)
            .map(Value::Int64)
            .map_err(|_| MongrelQueryError::Schema(format!("view value {v} exceeds i64 range"))),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::Utf8View(Some(v))
        | ScalarValue::LargeUtf8(Some(v)) => Ok(Value::Bytes(v.into_bytes())),
        ScalarValue::Binary(Some(v))
        | ScalarValue::BinaryView(Some(v))
        | ScalarValue::FixedSizeBinary(_, Some(v))
        | ScalarValue::LargeBinary(Some(v)) => Ok(Value::Bytes(v)),
        ScalarValue::Date32(Some(v))
        | ScalarValue::Time32Second(Some(v))
        | ScalarValue::Time32Millisecond(Some(v)) => Ok(Value::Int64(v as i64)),
        ScalarValue::Date64(Some(v))
        | ScalarValue::Time64Microsecond(Some(v))
        | ScalarValue::Time64Nanosecond(Some(v))
        | ScalarValue::TimestampSecond(Some(v), _)
        | ScalarValue::TimestampMillisecond(Some(v), _)
        | ScalarValue::TimestampMicrosecond(Some(v), _)
        | ScalarValue::TimestampNanosecond(Some(v), _)
        | ScalarValue::DurationSecond(Some(v))
        | ScalarValue::DurationMillisecond(Some(v))
        | ScalarValue::DurationMicrosecond(Some(v))
        | ScalarValue::DurationNanosecond(Some(v)) => Ok(Value::Int64(v)),
        ScalarValue::Dictionary(_, value) | ScalarValue::RunEndEncoded(_, _, value) => {
            scalar_to_core_value(*value)
        }
        ScalarValue::Decimal128(Some(v), _, _) => Ok(Value::Decimal(v)),
        other => Err(MongrelQueryError::Schema(format!(
            "view write routing cannot materialize {other:?}"
        ))),
    }
}

fn truncate_tables(session: &MongrelSession, db: &Arc<Database>, truncate: Truncate) -> Result<()> {
    let mut ops = Vec::new();
    for target in truncate.table_names {
        let table = object_name(&target.name)?;
        if let Some(entry) = db.external_table(&table) {
            return Err(external_table_write_error("TRUNCATE", &entry));
        }
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
    let changes = logical_changes(&ops);
    stage_or_apply(session, db, ops, changes, None)
}

fn stage_or_apply(
    session: &MongrelSession,
    db: &Arc<Database>,
    ops: Vec<PendingSqlOp>,
    changes: u64,
    last_insert_rowid: Option<u64>,
) -> Result<()> {
    if ops.is_empty() {
        session.sql_fn_state.record_changes(0, None);
        return Ok(());
    }
    if let Some(staged) = session.sql_txn.lock().as_mut() {
        staged.extend(ops);
        session
            .sql_fn_state
            .record_changes(changes, last_insert_rowid);
        return Ok(());
    }
    let external_tables = external_tables_to_refresh(db, &ops);
    apply_ops(session, db, ops)?;
    refresh_external_tables(session, db, &external_tables)?;
    session
        .sql_fn_state
        .record_changes(changes, last_insert_rowid);
    session.clear_cache();
    Ok(())
}

fn external_tables_in_ops(ops: &[PendingSqlOp]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for op in ops {
        if let PendingSqlOp::ExternalState { table, .. } = op {
            if seen.insert(table.clone()) {
                names.push(table.clone());
            }
        }
    }
    names
}

fn external_tables_to_refresh(db: &Arc<Database>, ops: &[PendingSqlOp]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();
    for name in external_tables_in_ops(ops) {
        if seen.insert(name.clone()) {
            names.push(name);
        }
    }
    for entry in db.external_tables() {
        if seen.insert(entry.name.clone()) {
            names.push(entry.name);
        }
    }
    names
}

fn refresh_external_tables(
    session: &MongrelSession,
    db: &Arc<Database>,
    names: &[String],
) -> Result<()> {
    for name in names {
        if let Some(entry) = db.external_table(name) {
            refresh_external_table_provider(session, db, &entry)?;
        }
    }
    Ok(())
}

fn logical_changes(ops: &[PendingSqlOp]) -> u64 {
    let external = ops
        .iter()
        .filter_map(|op| match op {
            PendingSqlOp::ExternalState { changes, .. } => Some(*changes),
            _ => None,
        })
        .sum::<u64>();
    let puts = ops
        .iter()
        .filter(|op| matches!(op, PendingSqlOp::Put { .. }))
        .count() as u64;
    let row_changes = if puts > 0 {
        puts
    } else {
        ops.iter()
            .filter(|op| matches!(op, PendingSqlOp::Delete { .. }))
            .count() as u64
    };
    external + row_changes
}

fn last_insert_pk(ops: &[PendingSqlOp], schema: &CoreSchema) -> Option<u64> {
    let pk = schema.primary_key()?;
    ops.iter().rev().find_map(|op| match op {
        PendingSqlOp::Put { cells, .. } => cells.iter().find_map(|(id, value)| {
            if *id == pk.id {
                match value {
                    Value::Int64(value) if *value >= 0 => Some(*value as u64),
                    _ => None,
                }
            } else {
                None
            }
        }),
        PendingSqlOp::Delete { .. } => None,
        PendingSqlOp::ExternalState { .. } => None,
    })
}

struct QueryExternalTriggerBridge {
    db: Arc<Database>,
    modules: Arc<ExternalModuleRegistry>,
}

impl ExternalTriggerBridge for QueryExternalTriggerBridge {
    fn apply_trigger_external_write(
        &self,
        entry: &ExternalTableEntry,
        base_state: Vec<u8>,
        op: ExternalTriggerWrite,
    ) -> mongreldb_core::Result<ExternalTriggerWriteResult> {
        let external_op = match op {
            ExternalTriggerWrite::Insert { cells, .. } => ExternalWriteOp::Insert {
                rows: vec![cells.into_iter().collect()],
            },
            ExternalTriggerWrite::UpdateByPk { pk, cells, .. } => {
                let mut rows = self
                    .modules
                    .external_table_rows_from_state(entry, &base_state)
                    .map_err(query_error_to_core)?;
                let pk_col = entry.declared_schema.primary_key().ok_or_else(|| {
                    mongreldb_core::MongrelError::InvalidArgument(format!(
                        "external trigger update target {:?} has no primary key",
                        entry.name
                    ))
                })?;
                let pk_key = pk.encode_key();
                let mut changed = 0_u64;
                for row in &mut rows {
                    if row
                        .get(&pk_col.id)
                        .is_some_and(|value| value.encode_key() == pk_key)
                    {
                        for (column_id, value) in &cells {
                            row.insert(*column_id, value.clone());
                        }
                        changed = changed.saturating_add(1);
                    }
                }
                if changed == 0 {
                    return Err(mongreldb_core::MongrelError::NotFound(format!(
                        "external trigger update target {:?} row not found",
                        entry.name
                    )));
                }
                ExternalWriteOp::ReplaceRows {
                    rows,
                    changes: changed,
                }
            }
            ExternalTriggerWrite::DeleteByPk { pk, .. } => {
                let rows = self
                    .modules
                    .external_table_rows_from_state(entry, &base_state)
                    .map_err(query_error_to_core)?;
                let pk_col = entry.declared_schema.primary_key().ok_or_else(|| {
                    mongreldb_core::MongrelError::InvalidArgument(format!(
                        "external trigger delete target {:?} has no primary key",
                        entry.name
                    ))
                })?;
                let pk_key = pk.encode_key();
                let before = rows.len();
                let rows = rows
                    .into_iter()
                    .filter(|row| {
                        !row.get(&pk_col.id)
                            .is_some_and(|value| value.encode_key() == pk_key)
                    })
                    .collect::<Vec<_>>();
                let changes = before.saturating_sub(rows.len()) as u64;
                if changes == 0 {
                    return Err(mongreldb_core::MongrelError::NotFound(format!(
                        "external trigger delete target {:?} row not found",
                        entry.name
                    )));
                }
                ExternalWriteOp::ReplaceRows { rows, changes }
            }
        };
        let (state, _result, base_writes) = self
            .modules
            .external_table_write(&self.db, entry, base_state, external_op)
            .map_err(query_error_to_core)?;
        Ok(ExternalTriggerWriteResult {
            state,
            base_writes: base_writes
                .into_iter()
                .map(core_base_write_from_query)
                .collect(),
        })
    }
}

fn core_base_write_from_query(op: ExternalBaseWrite) -> ExternalTriggerBaseWrite {
    match op {
        ExternalBaseWrite::Put { table, cells } => ExternalTriggerBaseWrite::Put { table, cells },
        ExternalBaseWrite::Delete { table, row_id } => ExternalTriggerBaseWrite::Delete {
            table,
            row_id: RowId(row_id),
        },
    }
}

fn query_error_to_core(err: MongrelQueryError) -> mongreldb_core::MongrelError {
    match err {
        MongrelQueryError::Core(err) => err,
        MongrelQueryError::Schema(msg) => mongreldb_core::MongrelError::InvalidArgument(msg),
        MongrelQueryError::Arrow(msg) | MongrelQueryError::DataFusion(msg) => {
            mongreldb_core::MongrelError::Other(msg)
        }
    }
}

fn apply_ops(session: &MongrelSession, db: &Arc<Database>, ops: Vec<PendingSqlOp>) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }
    let bridge = QueryExternalTriggerBridge {
        db: Arc::clone(db),
        modules: Arc::clone(&session.external_modules),
    };
    db.transaction_with_external_trigger_bridge(&bridge, |tx| {
        for op in ops {
            match op {
                PendingSqlOp::Put { table, cells } => {
                    tx.put(&table, cells)?;
                }
                PendingSqlOp::Delete { table, row_id } => {
                    tx.delete(&table, row_id)?;
                }
                PendingSqlOp::ExternalState { table, state, .. } => {
                    tx.put_external_state(&table, state)?;
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
        constraints: Default::default(),
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

fn expr_to_untyped_value(expr: &Expr) -> Result<Value> {
    match expr {
        Expr::Value(v) => sql_value_to_value(&v.value, None),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr_to_untyped_value(expr)? {
            Value::Int64(v) => Ok(Value::Int64(v.saturating_neg())),
            Value::Float64(v) => Ok(Value::Float64(-v)),
            _ => Err(MongrelQueryError::Schema(
                "unary minus requires a numeric literal".into(),
            )),
        },
        _ => Err(MongrelQueryError::Schema(format!(
            "INSERT values must be literals, got {expr}"
        ))),
    }
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

fn trigger_values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int64(a), Value::Int64(b)) => a == b,
        (Value::Float64(a), Value::Float64(b)) => a.to_bits() == b.to_bits(),
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Embedding(a), Value::Embedding(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|(a, b)| a.to_bits() == b.to_bits())
        }
        _ => false,
    }
}

fn trigger_message(value: Value) -> String {
    match value {
        Value::Null => "NULL".into(),
        Value::Bool(value) => value.to_string(),
        Value::Int64(value) => value.to_string(),
        Value::Float64(value) => value.to_string(),
        Value::Bytes(value) => String::from_utf8_lossy(&value).into_owned(),
        Value::Embedding(value) => format!("{value:?}"),
        Value::Decimal(value) => value.to_string(),
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
        (Value::Decimal(a), Value::Decimal(b)) => a.partial_cmp(b),
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
    match db.table(table) {
        Ok(handle) => {
            let schema = {
                let guard = handle.lock();
                guard.schema().clone()
            };
            Ok(schema)
        }
        Err(e) if matches!(e, mongreldb_core::MongrelError::NotFound(_)) => db
            .external_table(table)
            .map(|entry| entry.declared_schema)
            .ok_or_else(|| e.into()),
        Err(e) => Err(e.into()),
    }
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
        session,
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
        session,
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
    let mut guard = handle.lock();
    // A deferred bulk load leaves HOT unbuilt; complete it before the point
    // lookup (Phase 14.7 lazy contract).
    guard.ensure_indexes_complete()?;
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
    Ok(value.trim_matches('"').trim_matches('`').trim_matches('\''))
}

fn parse_procedure_json<'a>(sql: &'a str, lower: &str, prefix: &str) -> Result<(&'a str, &'a str)> {
    let body = sql[prefix.len()..].trim();
    let lower_body = &lower[prefix.len()..];
    let marker = " as json ";
    let idx = lower_body
        .find(marker)
        .ok_or_else(|| MongrelQueryError::Schema("expected AS JSON '<procedure>'".into()))?;
    let name = strip_identifier(&body[..idx])?;
    let json = unquote_sql_string(&body[idx + marker.len()..])?;
    Ok((name, json))
}

fn parse_call_json<'a>(sql: &'a str, lower: &'a str) -> Result<(&'a str, HashMap<String, Value>)> {
    let body = sql[5..].trim().trim_end_matches(';').trim();
    let lower_body = lower[5..].trim().trim_end_matches(';').trim();
    let marker = "(json ";
    let idx = lower_body
        .find(marker)
        .ok_or_else(|| MongrelQueryError::Schema("expected CALL name(JSON '<args>')".into()))?;
    let name = strip_identifier(&body[..idx])?;
    let rest = body[idx + marker.len()..].trim();
    let rest = rest
        .strip_suffix(')')
        .ok_or_else(|| MongrelQueryError::Schema("expected closing ')' for CALL".into()))?
        .trim();
    let json = unquote_sql_string(rest)?;
    let raw: serde_json::Value =
        serde_json::from_str(json).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    let obj = raw
        .as_object()
        .ok_or_else(|| MongrelQueryError::Schema("CALL args JSON must be an object".into()))?;
    let mut args = HashMap::new();
    for (key, value) in obj {
        args.insert(key.clone(), json_value_to_core(value)?);
    }
    Ok((name, args))
}

fn unquote_sql_string(value: &str) -> Result<&str> {
    let value = value.trim().trim_end_matches(';').trim();
    if value.len() < 2 || !value.starts_with('\'') || !value.ends_with('\'') {
        return Err(MongrelQueryError::Schema(
            "expected single-quoted JSON".into(),
        ));
    }
    Ok(&value[1..value.len() - 1])
}

fn procedure_from_json(name: &str, json: &str) -> Result<StoredProcedure> {
    let parsed: StoredProcedure =
        serde_json::from_str(json).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    StoredProcedure::new(name, parsed.mode, parsed.params, parsed.body, 0)
        .map_err(MongrelQueryError::from)
}

fn json_value_to_core(value: &serde_json::Value) -> Result<Value> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float64(value))
            } else {
                Err(MongrelQueryError::Schema("unsupported JSON number".into()))
            }
        }
        serde_json::Value::String(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Err(
            MongrelQueryError::Schema("procedure args only support scalar JSON values".into()),
        ),
    }
}

fn procedure_output_json(output: &ProcedureCallOutput) -> serde_json::Value {
    match output {
        ProcedureCallOutput::Null => serde_json::Value::Null,
        ProcedureCallOutput::Scalar(value) => core_value_json(value),
        ProcedureCallOutput::Row(row) => procedure_row_json(row),
        ProcedureCallOutput::Rows(rows) => {
            serde_json::Value::Array(rows.iter().map(procedure_row_json).collect())
        }
        ProcedureCallOutput::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), procedure_output_json(value)))
                .collect(),
        ),
        ProcedureCallOutput::Array(values) => {
            serde_json::Value::Array(values.iter().map(procedure_output_json).collect())
        }
    }
}

fn procedure_row_json(row: &ProcedureCallRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    if let Some(row_id) = row.row_id {
        obj.insert(
            "row_id".into(),
            serde_json::Value::String(row_id.0.to_string()),
        );
    }
    let columns = row
        .columns
        .iter()
        .map(|(id, value)| (id.to_string(), core_value_json(value)))
        .collect();
    obj.insert("columns".into(), serde_json::Value::Object(columns));
    serde_json::Value::Object(obj)
}

fn core_value_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int64(value) => serde_json::Value::from(*value),
        Value::Float64(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bytes(value) => String::from_utf8(value.clone())
            .map(serde_json::Value::String)
            .unwrap_or_else(|_| {
                serde_json::Value::Array(
                    value.iter().map(|b| serde_json::Value::from(*b)).collect(),
                )
            }),
        Value::Embedding(value) => serde_json::Value::Array(
            value
                .iter()
                .map(|v| {
                    serde_json::Number::from_f64(*v as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::Decimal(value) => serde_json::Value::String(value.to_string()),
    }
}

fn compact_all(db: &Arc<Database>) -> Result<()> {
    for table in db.table_names() {
        db.table(&table)?.lock().compact()?;
    }
    let _ = db.gc()?;
    Ok(())
}

fn analyze_all(db: &Arc<Database>) -> Result<()> {
    for table in db.table_names() {
        let handle = db.table(&table)?;
        handle.lock().ensure_indexes_complete()?;
    }
    Ok(())
}

fn reindex(db: &Arc<Database>, target: Option<&str>) -> Result<()> {
    match target {
        None => {
            for table in db.table_names() {
                let handle = db.table(&table)?;
                let mut guard = handle.lock();
                guard.ensure_indexes_complete()?;
                guard.compact()?;
            }
        }
        Some(name) => {
            if db.table_id(name).is_ok() {
                let handle = db.table(name)?;
                let mut guard = handle.lock();
                guard.ensure_indexes_complete()?;
                guard.compact()?;
            } else if let Some(table) = find_index_table(db, name)? {
                let handle = db.table(&table)?;
                let mut guard = handle.lock();
                guard.ensure_indexes_complete()?;
                guard.compact()?;
            } else {
                return Err(MongrelQueryError::Schema(format!(
                    "REINDEX target {name:?} is not a table or index"
                )));
            }
        }
    }
    let _ = db.gc()?;
    Ok(())
}

fn parse_vacuum_into<'a>(sql: &'a str, lower: &str) -> Result<&'a str> {
    let marker = "vacuum into ";
    let body = sql[marker.len()..].trim().trim_end_matches(';').trim();
    let lower_body = lower[marker.len()..].trim().trim_end_matches(';').trim();
    if lower_body.is_empty() {
        return Err(MongrelQueryError::Schema(
            "VACUUM INTO requires a target directory path".into(),
        ));
    }
    if body.starts_with('\'') {
        unquote_sql_string(body)
    } else {
        strip_identifier(body)
    }
}

fn copy_database_dir(src: &Path, dest: &Path) -> Result<()> {
    let src = src
        .canonicalize()
        .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    if dest.exists() {
        return Err(MongrelQueryError::Schema(format!(
            "VACUUM INTO target already exists: {}",
            dest.display()
        )));
    }
    let parent = dest.parent().unwrap_or_else(|| Path::new("."));
    if parent.exists() {
        let parent = parent
            .canonicalize()
            .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        if parent.starts_with(&src) {
            return Err(MongrelQueryError::Schema(
                "VACUUM INTO target must not be inside the source database directory".into(),
            ));
        }
    }
    copy_dir_recursive(&src, dest)
}

fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<()> {
    fs::create_dir_all(dest).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    for entry in fs::read_dir(src).map_err(|e| MongrelQueryError::Schema(e.to_string()))? {
        let entry = entry.map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        let src_path = entry.path();
        let dest_path = dest.join(entry.file_name());
        let metadata = entry
            .metadata()
            .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        if metadata.is_dir() {
            copy_dir_recursive(&src_path, &dest_path)?;
        } else if metadata.is_file() {
            fs::copy(&src_path, &dest_path)
                .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        }
    }
    Ok(())
}

fn run_pragma(
    session: &MongrelSession,
    db: &Arc<Database>,
    sql: &str,
    lower: &str,
) -> Result<RecordBatch> {
    let body = sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .get(6..)
        .ok_or_else(|| MongrelQueryError::Schema("expected PRAGMA name".into()))?
        .trim();
    let lower_body = lower
        .trim()
        .trim_end_matches(';')
        .trim()
        .get(6..)
        .ok_or_else(|| MongrelQueryError::Schema("expected PRAGMA name".into()))?
        .trim();
    let (name, arg) = parse_pragma_body(body, lower_body)?;
    match name.as_str() {
        "table_info" => pragma_table_info(db, required_pragma_arg(&name, arg.as_deref())?),
        "table_xinfo" => pragma_table_xinfo(db, required_pragma_arg(&name, arg.as_deref())?),
        "table_list" => pragma_table_list(session, db, arg.as_deref()),
        "index_list" => pragma_index_list(session, db, required_pragma_arg(&name, arg.as_deref())?),
        "index_info" => pragma_index_info(session, db, required_pragma_arg(&name, arg.as_deref())?),
        "index_xinfo" => {
            pragma_index_xinfo(session, db, required_pragma_arg(&name, arg.as_deref())?)
        }
        "foreign_key_list" => {
            pragma_foreign_key_list(db, required_pragma_arg(&name, arg.as_deref())?)
        }
        "foreign_key_check" => pragma_foreign_key_check(db, arg.as_deref()),
        "database_list" => pragma_database_list(db),
        "function_list" => pragma_function_list(),
        "module_list" => pragma_module_list(session),
        "trigger_list" => pragma_trigger_list(db),
        "collation_list" => pragma_collation_list(),
        "compile_options" => pragma_compile_options(),
        "integrity_check" => pragma_check_batch(db, "integrity_check"),
        "quick_check" => pragma_check_batch(db, "quick_check"),
        "schema_version" => int_batch("schema_version", schema_version(db)),
        "user_version" => {
            if let Some(value) = parse_optional_i64(arg.as_deref())? {
                set_db_pragma_i64(db, "user_version", value)?;
            }
            int_batch("user_version", get_db_pragma_i64(db, "user_version")?)
        }
        "application_id" => {
            if let Some(value) = parse_optional_i64(arg.as_deref())? {
                set_db_pragma_i64(db, "application_id", value)?;
            }
            int_batch("application_id", get_db_pragma_i64(db, "application_id")?)
        }
        "data_version" => int_batch("data_version", db.visible_epoch().0 as i64),
        "foreign_keys" => int_batch("foreign_keys", 1),
        "query_only" => int_batch("query_only", 0),
        "journal_mode" => strings_batch("journal_mode", vec!["wal".to_string()]),
        "synchronous" => int_batch("synchronous", 1),
        "encoding" => strings_batch("encoding", vec!["UTF-8".to_string()]),
        "page_size" => int_batch("page_size", 4096),
        "page_count" => int_batch("page_count", db_page_count(db)?),
        "freelist_count" => int_batch("freelist_count", 0),
        "cache_size" => int_batch("cache_size", -2000),
        "automatic_index" => int_batch("automatic_index", 1),
        "defer_foreign_keys" => int_batch("defer_foreign_keys", 0),
        "recursive_triggers" => {
            if let Some(value) = parse_optional_i64(arg.as_deref())? {
                db.set_recursive_triggers(value != 0);
            }
            int_batch(
                "recursive_triggers",
                i64::from(db.trigger_config().recursive_triggers),
            )
        }
        "trusted_schema" => int_batch("trusted_schema", 0),
        "wal_checkpoint" => pragma_wal_checkpoint(db),
        "optimize" => {
            analyze_all(db)?;
            session.clear_cache();
            strings_batch("optimize", vec!["ok".to_string()])
        }
        _other => empty_batch(),
    }
}

fn parse_pragma_body(body: &str, lower_body: &str) -> Result<(String, Option<String>)> {
    let (raw_name, raw_arg) = if let Some(open) = lower_body.find('(') {
        let close = lower_body
            .rfind(')')
            .ok_or_else(|| MongrelQueryError::Schema("expected closing ')' in PRAGMA".into()))?;
        (&body[..open], Some(&body[open + 1..close]))
    } else if let Some(eq) = lower_body.find('=') {
        (&body[..eq], Some(&body[eq + 1..]))
    } else {
        (body, None)
    };
    let mut name = raw_name.trim().to_ascii_lowercase();
    if let Some((schema, local)) = name.split_once('.') {
        if schema != "main" {
            return Err(MongrelQueryError::Schema(format!(
                "unsupported PRAGMA schema qualifier {schema:?}"
            )));
        }
        name = local.to_string();
    }
    let arg = raw_arg
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(strip_identifier)
        .transpose()?
        .map(str::to_string);
    Ok((name, arg))
}

fn required_pragma_arg<'a>(name: &str, arg: Option<&'a str>) -> Result<&'a str> {
    arg.ok_or_else(|| MongrelQueryError::Schema(format!("expected PRAGMA {name}(<name>)")))
}

fn schema_version(db: &Arc<Database>) -> i64 {
    db.table_names()
        .into_iter()
        .filter_map(|table| {
            table_schema(db, &table)
                .ok()
                .map(|schema| schema.schema_id as i64)
        })
        .max()
        .unwrap_or(0)
}

fn parse_optional_i64(value: Option<&str>) -> Result<Option<i64>> {
    value
        .map(|value| {
            value
                .trim()
                .parse::<i64>()
                .map_err(|e| MongrelQueryError::Schema(format!("invalid PRAGMA integer: {e}")))
        })
        .transpose()
}

fn db_pragma_file(db: &Arc<Database>) -> std::path::PathBuf {
    db.root().join("_meta").join("sql_pragmas.json")
}

fn get_db_pragma_i64(db: &Arc<Database>, key: &str) -> Result<i64> {
    let path = db_pragma_file(db);
    let Ok(bytes) = fs::read(&path) else {
        return Ok(0);
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    Ok(value.get(key).and_then(|v| v.as_i64()).unwrap_or(0))
}

fn set_db_pragma_i64(db: &Arc<Database>, key: &str, value: i64) -> Result<()> {
    let path = db_pragma_file(db);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    }
    let mut object = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    object.insert(key.to_string(), serde_json::Value::from(value));
    let bytes = serde_json::to_vec_pretty(&serde_json::Value::Object(object))
        .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
    fs::write(path, bytes).map_err(|e| MongrelQueryError::Schema(e.to_string()))
}

fn db_page_count(db: &Arc<Database>) -> Result<i64> {
    let bytes = dir_size(db.root())?;
    Ok(bytes.div_ceil(4096) as i64)
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0_u64;
    let Ok(entries) = fs::read_dir(path) else {
        return Ok(0);
    };
    for entry in entries {
        let entry = entry.map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        let metadata = entry
            .metadata()
            .map_err(|e| MongrelQueryError::Schema(e.to_string()))?;
        if metadata.is_dir() {
            total = total.saturating_add(dir_size(&entry.path())?);
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
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
    let not_null: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(!c.flags.contains(ColumnFlags::NULLABLE)))
        .collect();
    let dflt_value: Vec<Option<String>> = schema.columns.iter().map(|_| None).collect();
    let pk: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(c.flags.contains(ColumnFlags::PRIMARY_KEY)))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("notnull", ArrowDataType::Int64, false),
            Field::new("dflt_value", ArrowDataType::Utf8, true),
            Field::new("pk", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(not_null)),
            Arc::new(StringArray::from(dflt_value)),
            Arc::new(Int64Array::from(pk)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_table_xinfo(db: &Arc<Database>, table: &str) -> Result<RecordBatch> {
    let schema = table_schema(db, table)?;
    let hidden_names = db
        .external_table(table)
        .map(|entry| entry.hidden_columns)
        .unwrap_or_default();
    let cid: Vec<i64> = (0..schema.columns.len()).map(|i| i as i64).collect();
    let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let types: Vec<String> = schema
        .columns
        .iter()
        .map(|c| format!("{:?}", c.ty))
        .collect();
    let not_null: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(!c.flags.contains(ColumnFlags::NULLABLE)))
        .collect();
    let dflt_value: Vec<Option<String>> = schema.columns.iter().map(|_| None).collect();
    let pk: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(c.flags.contains(ColumnFlags::PRIMARY_KEY)))
        .collect();
    let hidden: Vec<i64> = schema
        .columns
        .iter()
        .map(|c| i64::from(hidden_names.iter().any(|name| name == &c.name)))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("notnull", ArrowDataType::Int64, false),
            Field::new("dflt_value", ArrowDataType::Utf8, true),
            Field::new("pk", ArrowDataType::Int64, false),
            Field::new("hidden", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(types)),
            Arc::new(Int64Array::from(not_null)),
            Arc::new(StringArray::from(dflt_value)),
            Arc::new(Int64Array::from(pk)),
            Arc::new(Int64Array::from(hidden)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_table_list(
    session: &MongrelSession,
    db: &Arc<Database>,
    filter: Option<&str>,
) -> Result<RecordBatch> {
    let mut schema_name = Vec::new();
    let mut names = Vec::new();
    let mut ty = Vec::new();
    let mut ncol = Vec::new();
    let mut wr = Vec::new();
    let mut strict = Vec::new();
    let filter = filter.map(str::to_ascii_lowercase);

    for table in db.table_names() {
        if filter
            .as_deref()
            .is_some_and(|needle| table.to_ascii_lowercase() != needle)
        {
            continue;
        }
        let table_schema = table_schema(db, &table)?;
        schema_name.push("main".to_string());
        names.push(table);
        ty.push("table".to_string());
        ncol.push(table_schema.columns.len() as i64);
        wr.push(0_i64);
        strict.push(0_i64);
    }

    for table in db.external_tables() {
        if filter
            .as_deref()
            .is_some_and(|needle| table.name.to_ascii_lowercase() != needle)
        {
            continue;
        }
        schema_name.push("main".to_string());
        names.push(table.name);
        ty.push("external".to_string());
        ncol.push(table.declared_schema.columns.len() as i64);
        wr.push(0_i64);
        strict.push(0_i64);
    }

    for (view, _) in session.views.lock().iter() {
        if filter
            .as_deref()
            .is_some_and(|needle| view.to_ascii_lowercase() != needle)
        {
            continue;
        }
        schema_name.push("main".to_string());
        names.push(view.clone());
        ty.push("view".to_string());
        ncol.push(0_i64);
        wr.push(0_i64);
        strict.push(0_i64);
    }

    let mut order = (0..names.len()).collect::<Vec<_>>();
    order.sort_by(|a, b| names[*a].cmp(&names[*b]));
    let schema_name: Vec<String> = order.iter().map(|i| schema_name[*i].clone()).collect();
    let names: Vec<String> = order.iter().map(|i| names[*i].clone()).collect();
    let ty: Vec<String> = order.iter().map(|i| ty[*i].clone()).collect();
    let ncol: Vec<i64> = order.iter().map(|i| ncol[*i]).collect();
    let wr: Vec<i64> = order.iter().map(|i| wr[*i]).collect();
    let strict: Vec<i64> = order.iter().map(|i| strict[*i]).collect();

    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("schema", ArrowDataType::Utf8, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("ncol", ArrowDataType::Int64, false),
            Field::new("wr", ArrowDataType::Int64, false),
            Field::new("strict", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(schema_name)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(ty)),
            Arc::new(Int64Array::from(ncol)),
            Arc::new(Int64Array::from(wr)),
            Arc::new(Int64Array::from(strict)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_index_list(
    session: &MongrelSession,
    db: &Arc<Database>,
    table: &str,
) -> Result<RecordBatch> {
    if let Some(entry) = db.external_table(table) {
        let indexes = session.external_modules.external_table_indexes(&entry)?;
        let seq: Vec<i64> = (0..indexes.len()).map(|i| i as i64).collect();
        let names: Vec<String> = indexes.iter().map(|i| i.name.clone()).collect();
        let unique: Vec<i64> = indexes.iter().map(|i| i64::from(i.unique)).collect();
        let origin: Vec<String> = indexes.iter().map(|_| "m".to_string()).collect();
        let partial: Vec<i64> = indexes.iter().map(|i| i64::from(i.partial)).collect();
        return index_list_batch(seq, names, unique, origin, partial);
    }
    let schema = table_schema(db, table)?;
    let seq: Vec<i64> = (0..schema.indexes.len()).map(|i| i as i64).collect();
    let names: Vec<String> = schema.indexes.iter().map(|i| i.name.clone()).collect();
    let unique: Vec<i64> = schema.indexes.iter().map(|_| 0).collect();
    let origin: Vec<String> = schema.indexes.iter().map(|_| "c".to_string()).collect();
    let partial: Vec<i64> = schema.indexes.iter().map(|_| 0).collect();
    index_list_batch(seq, names, unique, origin, partial)
}

fn index_list_batch(
    seq: Vec<i64>,
    names: Vec<String>,
    unique: Vec<i64>,
    origin: Vec<String>,
    partial: Vec<i64>,
) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("unique", ArrowDataType::Int64, false),
            Field::new("origin", ArrowDataType::Utf8, false),
            Field::new("partial", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seq)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(unique)),
            Arc::new(StringArray::from(origin)),
            Arc::new(Int64Array::from(partial)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_index_xinfo(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: &str,
) -> Result<RecordBatch> {
    if let Some((entry, def)) = find_external_module_index(session, db, index)? {
        return pragma_external_index_xinfo(&entry, &def);
    }
    let table = find_index_table(db, index)?.ok_or_else(|| {
        MongrelQueryError::Schema(format!("index {index:?} does not exist in this database"))
    })?;
    let schema = table_schema(db, &table)?;
    let defs = index_defs(&schema, index);
    let seqno: Vec<i64> = (0..defs.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = defs.iter().map(|idx| idx.column_id as i64).collect();
    let names: Vec<String> = defs
        .iter()
        .map(|idx| column_name(&schema, idx.column_id))
        .collect();
    let desc: Vec<i64> = defs.iter().map(|_| 0_i64).collect();
    let coll: Vec<String> = defs.iter().map(|_| "BINARY".to_string()).collect();
    let key: Vec<i64> = defs.iter().map(|_| 1_i64).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("desc", ArrowDataType::Int64, false),
            Field::new("coll", ArrowDataType::Utf8, false),
            Field::new("key", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(desc)),
            Arc::new(StringArray::from(coll)),
            Arc::new(Int64Array::from(key)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_index_info(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: &str,
) -> Result<RecordBatch> {
    if let Some((entry, def)) = find_external_module_index(session, db, index)? {
        return pragma_external_index_info(&entry, &def);
    }
    let table = find_index_table(db, index)?.ok_or_else(|| {
        MongrelQueryError::Schema(format!("index {index:?} does not exist in this database"))
    })?;
    let schema = table_schema(db, &table)?;
    let defs = index_defs(&schema, index);
    let seqno: Vec<i64> = (0..defs.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = defs.iter().map(|idx| idx.column_id as i64).collect();
    let names: Vec<String> = defs
        .iter()
        .map(|idx| {
            schema
                .columns
                .iter()
                .find(|col| col.id == idx.column_id)
                .map(|col| col.name.clone())
                .unwrap_or_default()
        })
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn find_external_module_index(
    session: &MongrelSession,
    db: &Arc<Database>,
    index: &str,
) -> Result<Option<(ExternalTableEntry, ExternalModuleIndex)>> {
    for entry in db.external_tables() {
        if let Some(def) = session
            .external_modules
            .external_table_indexes(&entry)?
            .into_iter()
            .find(|def| def.name == index)
        {
            return Ok(Some((entry, def)));
        }
    }
    Ok(None)
}

fn pragma_external_index_xinfo(
    entry: &ExternalTableEntry,
    def: &ExternalModuleIndex,
) -> Result<RecordBatch> {
    let seqno: Vec<i64> = (0..def.column_ids.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = def.column_ids.iter().map(|id| *id as i64).collect();
    let names: Vec<String> = def
        .column_ids
        .iter()
        .map(|id| column_name(&entry.declared_schema, *id))
        .collect();
    let desc: Vec<i64> = def.column_ids.iter().map(|_| 0_i64).collect();
    let coll: Vec<String> = def
        .column_ids
        .iter()
        .map(|_| "BINARY".to_string())
        .collect();
    let key: Vec<i64> = def.column_ids.iter().map(|_| 1_i64).collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("desc", ArrowDataType::Int64, false),
            Field::new("coll", ArrowDataType::Utf8, false),
            Field::new("key", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(desc)),
            Arc::new(StringArray::from(coll)),
            Arc::new(Int64Array::from(key)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_external_index_info(
    entry: &ExternalTableEntry,
    def: &ExternalModuleIndex,
) -> Result<RecordBatch> {
    let seqno: Vec<i64> = (0..def.column_ids.len()).map(|i| i as i64).collect();
    let cid: Vec<i64> = def.column_ids.iter().map(|id| *id as i64).collect();
    let names: Vec<String> = def
        .column_ids
        .iter()
        .map(|id| column_name(&entry.declared_schema, *id))
        .collect();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seqno", ArrowDataType::Int64, false),
            Field::new("cid", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seqno)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn index_defs<'a>(schema: &'a CoreSchema, index: &str) -> Vec<&'a IndexDef> {
    let prefix = format!("{index}_");
    schema
        .indexes
        .iter()
        .filter(|idx| idx.name == index || idx.name.starts_with(&prefix))
        .collect()
}

fn pragma_foreign_key_list(db: &Arc<Database>, table: &str) -> Result<RecordBatch> {
    let schema = table_schema(db, table)?;
    let mut id = Vec::new();
    let mut seq = Vec::new();
    let mut ref_table = Vec::new();
    let mut from = Vec::new();
    let mut to = Vec::new();
    let mut on_update = Vec::new();
    let mut on_delete = Vec::new();
    let mut match_kind = Vec::new();
    for fk in &schema.constraints.foreign_keys {
        let parent = table_schema(db, &fk.ref_table).ok();
        for (i, (from_id, to_id)) in fk.columns.iter().zip(fk.ref_columns.iter()).enumerate() {
            id.push(fk.id as i64);
            seq.push(i as i64);
            ref_table.push(fk.ref_table.clone());
            from.push(column_name(&schema, *from_id));
            to.push(
                parent
                    .as_ref()
                    .map(|s| column_name(s, *to_id))
                    .unwrap_or_else(|| to_id.to_string()),
            );
            on_update.push("NO ACTION".to_string());
            on_delete.push(format_fk_action(fk.on_delete).to_string());
            match_kind.push("NONE".to_string());
        }
    }
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("table", ArrowDataType::Utf8, false),
            Field::new("from", ArrowDataType::Utf8, false),
            Field::new("to", ArrowDataType::Utf8, false),
            Field::new("on_update", ArrowDataType::Utf8, false),
            Field::new("on_delete", ArrowDataType::Utf8, false),
            Field::new("match", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(id)) as ArrayRef,
            Arc::new(Int64Array::from(seq)),
            Arc::new(StringArray::from(ref_table)),
            Arc::new(StringArray::from(from)),
            Arc::new(StringArray::from(to)),
            Arc::new(StringArray::from(on_update)),
            Arc::new(StringArray::from(on_delete)),
            Arc::new(StringArray::from(match_kind)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_foreign_key_check(db: &Arc<Database>, table: Option<&str>) -> Result<RecordBatch> {
    let mut child_table = Vec::new();
    let mut rowid = Vec::new();
    let mut parent_table = Vec::new();
    let mut fkid = Vec::new();
    let tables = match table {
        Some(table) => vec![table.to_string()],
        None => db.table_names(),
    };

    for table_name in tables {
        let (schema, rows) = visible_rows(db, &table_name)?;
        for fk in &schema.constraints.foreign_keys {
            let Ok((_parent_schema, parent_rows)) = visible_rows(db, &fk.ref_table) else {
                for row in &rows {
                    if fk_row_is_checkable(row, &fk.columns) {
                        child_table.push(table_name.clone());
                        rowid.push(row.row_id.0 as i64);
                        parent_table.push(fk.ref_table.clone());
                        fkid.push(fk.id as i64);
                    }
                }
                continue;
            };
            for row in &rows {
                let values = fk
                    .columns
                    .iter()
                    .map(|column| row.columns.get(column).cloned().unwrap_or(Value::Null))
                    .collect::<Vec<_>>();
                if values.iter().any(|value| matches!(value, Value::Null)) {
                    continue;
                }
                let parent_exists = parent_rows.iter().any(|parent| {
                    fk.ref_columns
                        .iter()
                        .zip(values.iter())
                        .all(|(column, value)| {
                            parent.columns.get(column).unwrap_or(&Value::Null) == value
                        })
                });
                if !parent_exists {
                    child_table.push(table_name.clone());
                    rowid.push(row.row_id.0 as i64);
                    parent_table.push(fk.ref_table.clone());
                    fkid.push(fk.id as i64);
                }
            }
        }
    }

    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("table", ArrowDataType::Utf8, false),
            Field::new("rowid", ArrowDataType::Int64, false),
            Field::new("parent", ArrowDataType::Utf8, false),
            Field::new("fkid", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(child_table)) as ArrayRef,
            Arc::new(Int64Array::from(rowid)),
            Arc::new(StringArray::from(parent_table)),
            Arc::new(Int64Array::from(fkid)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn fk_row_is_checkable(row: &Row, columns: &[u16]) -> bool {
    columns
        .iter()
        .all(|column| !matches!(row.columns.get(column), None | Some(Value::Null)))
}

fn column_name(schema: &CoreSchema, id: u16) -> String {
    schema
        .columns
        .iter()
        .find(|col| col.id == id)
        .map(|col| col.name.clone())
        .unwrap_or_else(|| id.to_string())
}

fn format_fk_action(action: mongreldb_core::constraint::FkAction) -> &'static str {
    match action {
        mongreldb_core::constraint::FkAction::Restrict => "RESTRICT",
        mongreldb_core::constraint::FkAction::Cascade => "CASCADE",
        mongreldb_core::constraint::FkAction::SetNull => "SET NULL",
    }
}

fn pragma_database_list(db: &Arc<Database>) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("file", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0])) as ArrayRef,
            Arc::new(StringArray::from(vec!["main"])),
            Arc::new(StringArray::from(vec![db.root().display().to_string()])),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_function_list() -> Result<RecordBatch> {
    let mut rows = crate::extended_sql_functions::extended_sql_function_names()
        .into_iter()
        .filter(|name| {
            *name != "json_each"
                && *name != "json_tree"
                && *name != "jsonb_each"
                && *name != "jsonb_tree"
                && *name != "series"
        })
        .map(|name| (name.to_string(), "s".to_string(), -1_i64))
        .collect::<Vec<_>>();
    rows.push(("ann_search".to_string(), "s".to_string(), 3));
    rows.push(("sparse_match".to_string(), "s".to_string(), 3));
    rows.push(("rtree_intersects".to_string(), "s".to_string(), 8));
    rows.push(("json_each".to_string(), "t".to_string(), 2));
    rows.push(("json_tree".to_string(), "t".to_string(), 2));
    rows.push(("jsonb_each".to_string(), "t".to_string(), 2));
    rows.push(("jsonb_tree".to_string(), "t".to_string(), 2));
    rows.push(("series".to_string(), "t".to_string(), 3));
    for (name, narg) in [
        ("avg", 1),
        ("count", -1),
        ("group_concat", -1),
        ("max", -1),
        ("median", 1),
        ("min", -1),
        ("percentile", 2),
        ("percentile_cont", 2),
        ("percentile_disc", 2),
        ("string_agg", 2),
        ("sum", 1),
        ("total", 1),
    ] {
        rows.push((name.to_string(), "w".to_string(), narg));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let names = rows.iter().map(|r| r.0.clone()).collect::<Vec<_>>();
    let builtin = rows.iter().map(|_| 1_i64).collect::<Vec<_>>();
    let kind = rows.iter().map(|r| r.1.clone()).collect::<Vec<_>>();
    let enc = rows.iter().map(|_| "utf8".to_string()).collect::<Vec<_>>();
    let narg = rows.iter().map(|r| r.2).collect::<Vec<_>>();
    let flags = rows.iter().map(|_| 0_i64).collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("builtin", ArrowDataType::Int64, false),
            Field::new("type", ArrowDataType::Utf8, false),
            Field::new("enc", ArrowDataType::Utf8, false),
            Field::new("narg", ArrowDataType::Int64, false),
            Field::new("flags", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(builtin)),
            Arc::new(StringArray::from(kind)),
            Arc::new(StringArray::from(enc)),
            Arc::new(Int64Array::from(narg)),
            Arc::new(Int64Array::from(flags)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_module_list(session: &MongrelSession) -> Result<RecordBatch> {
    strings_batch("name", session.external_modules.names())
}

fn pragma_trigger_list(db: &Arc<Database>) -> Result<RecordBatch> {
    let triggers = db.triggers();
    let seq = (0..triggers.len() as i64).collect::<Vec<_>>();
    let names = triggers
        .iter()
        .map(|trigger| trigger.name.clone())
        .collect::<Vec<_>>();
    let target_type = triggers
        .iter()
        .map(|trigger| match &trigger.target {
            TriggerTarget::Table(_) => "table".to_string(),
            TriggerTarget::View(_) => "view".to_string(),
        })
        .collect::<Vec<_>>();
    let target = triggers
        .iter()
        .map(|trigger| match &trigger.target {
            TriggerTarget::Table(name) | TriggerTarget::View(name) => name.clone(),
        })
        .collect::<Vec<_>>();
    let timing = triggers
        .iter()
        .map(|trigger| trigger_timing_name(trigger.timing).to_string())
        .collect::<Vec<_>>();
    let event = triggers
        .iter()
        .map(|trigger| trigger_event_name(trigger.event).to_string())
        .collect::<Vec<_>>();
    let enabled = triggers
        .iter()
        .map(|trigger| i64::from(trigger.enabled))
        .collect::<Vec<_>>();
    let created_epoch = triggers
        .iter()
        .map(|trigger| trigger.created_epoch as i64)
        .collect::<Vec<_>>();
    let updated_epoch = triggers
        .iter()
        .map(|trigger| trigger.updated_epoch as i64)
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
            Field::new("target_type", ArrowDataType::Utf8, false),
            Field::new("target", ArrowDataType::Utf8, false),
            Field::new("timing", ArrowDataType::Utf8, false),
            Field::new("event", ArrowDataType::Utf8, false),
            Field::new("enabled", ArrowDataType::Int64, false),
            Field::new("created_epoch", ArrowDataType::Int64, false),
            Field::new("updated_epoch", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(seq)) as ArrayRef,
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(target_type)),
            Arc::new(StringArray::from(target)),
            Arc::new(StringArray::from(timing)),
            Arc::new(StringArray::from(event)),
            Arc::new(Int64Array::from(enabled)),
            Arc::new(Int64Array::from(created_epoch)),
            Arc::new(Int64Array::from(updated_epoch)),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn trigger_timing_name(timing: TriggerTiming) -> &'static str {
    match timing {
        TriggerTiming::Before => "BEFORE",
        TriggerTiming::After => "AFTER",
        TriggerTiming::InsteadOf => "INSTEAD OF",
    }
}

fn trigger_event_name(event: TriggerEvent) -> &'static str {
    match event {
        TriggerEvent::Insert => "INSERT",
        TriggerEvent::Update => "UPDATE",
        TriggerEvent::Delete => "DELETE",
    }
}

fn pragma_collation_list() -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("seq", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["BINARY"])),
        ],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn pragma_compile_options() -> Result<RecordBatch> {
    strings_batch(
        "compile_options",
        vec![
            "DATAFUSION_54".to_string(),
            "EXTENDED_SQL_FUNCTIONS".to_string(),
            "JSON_TABLE_FUNCTIONS".to_string(),
            "LOG_STRUCTURED_STORAGE".to_string(),
            "MVCC".to_string(),
            "WAL".to_string(),
        ],
    )
}

fn pragma_wal_checkpoint(db: &Arc<Database>) -> Result<RecordBatch> {
    for table in db.table_names() {
        let handle = db.table(&table)?;
        handle.lock().flush()?;
    }
    let _ = db.gc()?;
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("busy", ArrowDataType::Int64, false),
            Field::new("log", ArrowDataType::Int64, false),
            Field::new("checkpointed", ArrowDataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![0_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![0_i64])),
            Arc::new(Int64Array::from(vec![0_i64])),
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

fn pragma_check_batch(db: &Arc<Database>, column_name: &str) -> Result<RecordBatch> {
    let issues = db.check();
    let values = if issues.is_empty() {
        vec!["ok".to_string()]
    } else {
        issues
            .iter()
            .map(|issue| {
                format!(
                    "{}: {}: {}",
                    issue.severity, issue.table_name, issue.description
                )
            })
            .collect()
    };
    strings_batch(column_name, values)
}

fn int_batch(name: &str, value: i64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![Field::new(
            name,
            ArrowDataType::Int64,
            false,
        )])),
        vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
    )
    .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

fn empty_batch() -> Result<RecordBatch> {
    Ok(RecordBatch::new_empty(Arc::new(ArrowSchema::empty())))
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

fn json_batch(name: &str, values: Vec<String>) -> Result<RecordBatch> {
    strings_batch(name, values)
}
