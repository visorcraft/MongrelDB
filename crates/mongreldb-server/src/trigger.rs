use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_core::{Permission, StoredTrigger, TriggerDefinition, TriggerStep, TriggerTarget};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{request_principal, AppState, OptionalPrincipal};

#[derive(Deserialize)]
pub struct TriggerRequest {
    trigger: StoredTrigger,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
struct TriggerIdempotencyPayload<'a, T> {
    request: &'a T,
    binding: &'a TriggerPayloadBinding,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct TriggerTableBinding {
    name: String,
    table_id: u64,
    schema_id: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct TriggerPayloadBinding {
    security_version: u64,
    tables: Vec<TriggerTableBinding>,
}

#[derive(Clone, Debug, PartialEq)]
struct TriggerExecutionBinding {
    catalog_epoch: u64,
    existing_trigger: Option<StoredTrigger>,
    payload: TriggerPayloadBinding,
}

pub async fn list(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    Json(json!({ "triggers": state.db.triggers() })).into_response()
}

pub async fn describe(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    match state.db.trigger(&name) {
        Some(trigger) => Json(json!({ "trigger": trigger })).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            "TRIGGER_NOT_FOUND",
            "trigger not found",
        ),
    }
}

pub async fn create(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<TriggerRequest>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    let key = match request_idempotency_key(&headers, req.idempotency_key.as_deref()) {
        Ok(key) => key,
        Err(response) => return *response,
    };
    let trigger = match normalized(req.trigger) {
        Ok(trigger) => trigger,
        Err(failure) => {
            return error(
                StatusCode::BAD_REQUEST,
                "TRIGGER_VALIDATION",
                &failure.to_string(),
            )
        }
    };
    let effective_principal = request_principal(&state, &principal);
    let execution_binding = match preflight_trigger(
        &state,
        effective_principal.as_ref(),
        &trigger.name,
        Some(&trigger),
    ) {
        Ok(binding) => binding,
        Err(response) => return *response,
    };
    let owner = match crate::kit::idempotency_owner(&state, principal.as_ref()) {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let payload = trigger.clone();
    let idempotency_payload = TriggerIdempotencyPayload {
        request: &payload,
        binding: &execution_binding.payload,
    };
    crate::kit::idempotent_json_validated(
        &state,
        &owner,
        "trigger:create",
        key.as_deref(),
        &idempotency_payload,
        || {
            state
                .db
                .create_trigger_as_controlled(trigger, effective_principal.as_ref(), || {
                    validate_trigger_binding(
                        &state,
                        effective_principal.as_ref(),
                        &execution_binding,
                        &payload.name,
                    )
                })
                .map(|trigger| json!({ "status": "ok", "trigger": trigger }))
                .map_err(|error| {
                    crate::kit::idempotent_core_failure(
                        error,
                        StatusCode::BAD_REQUEST,
                        "TRIGGER_VALIDATION",
                    )
                })
        },
        |body| {
            validate_trigger_replay(
                &state,
                effective_principal.as_ref(),
                &execution_binding.payload,
                &payload.name,
                body,
                false,
            )
        },
    )
    .await
}

pub async fn replace(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
    Json(req): Json<TriggerRequest>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    let key = match request_idempotency_key(&headers, req.idempotency_key.as_deref()) {
        Ok(key) => key,
        Err(response) => return *response,
    };
    let mut trigger = req.trigger;
    trigger.name = name;
    let trigger = match normalized(trigger) {
        Ok(trigger) => trigger,
        Err(failure) => {
            return error(
                StatusCode::BAD_REQUEST,
                "TRIGGER_VALIDATION",
                &failure.to_string(),
            )
        }
    };
    let effective_principal = request_principal(&state, &principal);
    let execution_binding = match preflight_trigger(
        &state,
        effective_principal.as_ref(),
        &trigger.name,
        Some(&trigger),
    ) {
        Ok(binding) => binding,
        Err(response) => return *response,
    };
    let owner = match crate::kit::idempotency_owner(&state, principal.as_ref()) {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let payload = trigger.clone();
    let idempotency_payload = TriggerIdempotencyPayload {
        request: &payload,
        binding: &execution_binding.payload,
    };
    crate::kit::idempotent_json_validated(
        &state,
        &owner,
        "trigger:replace",
        key.as_deref(),
        &idempotency_payload,
        || {
            state
                .db
                .create_or_replace_trigger_as_controlled(
                    trigger,
                    effective_principal.as_ref(),
                    || {
                        validate_trigger_binding(
                            &state,
                            effective_principal.as_ref(),
                            &execution_binding,
                            &payload.name,
                        )
                    },
                )
                .map(|trigger| json!({ "status": "ok", "trigger": trigger }))
                .map_err(|error| {
                    crate::kit::idempotent_core_failure(
                        error,
                        StatusCode::BAD_REQUEST,
                        "TRIGGER_VALIDATION",
                    )
                })
        },
        |body| {
            validate_trigger_replay(
                &state,
                effective_principal.as_ref(),
                &execution_binding.payload,
                &payload.name,
                body,
                false,
            )
        },
    )
    .await
}

pub async fn drop_trigger(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    let key = match request_idempotency_key(&headers, None) {
        Ok(key) => key,
        Err(response) => return *response,
    };
    let effective_principal = request_principal(&state, &principal);
    let execution_binding =
        match preflight_trigger(&state, effective_principal.as_ref(), &name, None) {
            Ok(binding) => binding,
            Err(response) => return *response,
        };
    let owner = match crate::kit::idempotency_owner(&state, principal.as_ref()) {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let payload = name.clone();
    let drop_payload_binding = TriggerPayloadBinding {
        security_version: execution_binding.payload.security_version,
        tables: Vec::new(),
    };
    let idempotency_payload = TriggerIdempotencyPayload {
        request: &payload,
        binding: &drop_payload_binding,
    };
    crate::kit::idempotent_json_validated(
        &state,
        &owner,
        "trigger:drop",
        key.as_deref(),
        &idempotency_payload,
        || {
            state
                .db
                .drop_triggers_with_epoch_as_controlled(
                    std::slice::from_ref(&name),
                    effective_principal.as_ref(),
                    || {
                        validate_trigger_binding(
                            &state,
                            effective_principal.as_ref(),
                            &execution_binding,
                            &payload,
                        )
                    },
                )
                .map(|epoch| {
                    json!({
                        "status": "committed",
                        "epoch": epoch.0,
                        "epoch_text": epoch.0.to_string(),
                        "dropped_trigger": execution_binding.existing_trigger.clone(),
                        "resource_tables": execution_binding.payload.tables.clone(),
                    })
                })
                .map_err(|error| {
                    crate::kit::idempotent_core_failure(
                        error,
                        StatusCode::NOT_FOUND,
                        "TRIGGER_NOT_FOUND",
                    )
                })
        },
        |body| {
            let stored_tables = body
                .get("resource_tables")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .ok_or_else(|| {
                    Box::new(error(
                        StatusCode::CONFLICT,
                        "IDEMPOTENCY_KEY_REUSE_MISMATCH",
                        "stored trigger drop receipt has no valid resource binding",
                    ))
                })?;
            let replay_binding = TriggerPayloadBinding {
                security_version: drop_payload_binding.security_version,
                tables: stored_tables,
            };
            validate_trigger_replay(
                &state,
                effective_principal.as_ref(),
                &replay_binding,
                &payload,
                body,
                true,
            )
        },
    )
    .await
}

fn normalized(trigger: StoredTrigger) -> mongreldb_core::Result<StoredTrigger> {
    StoredTrigger::new(
        trigger.name,
        TriggerDefinition {
            target: trigger.target,
            timing: trigger.timing,
            event: trigger.event,
            update_of: trigger.update_of,
            target_columns: trigger.target_columns,
            when: trigger.when,
            program: trigger.program,
        },
        0,
    )
}

fn preflight_trigger(
    state: &AppState,
    principal: Option<&mongreldb_core::Principal>,
    name: &str,
    requested: Option<&StoredTrigger>,
) -> Result<TriggerExecutionBinding, Box<Response>> {
    for _ in 0..3 {
        let security_version = state.db.security_version();
        let catalog_epoch = state.db.catalog_snapshot().db_epoch;
        state
            .db
            .require_for(principal, &Permission::Ddl)
            .map_err(|failure| {
                Box::new(error(
                    crate::status_for_error(&failure),
                    "TRIGGER_VALIDATION",
                    &failure.to_string(),
                ))
            })?;
        let existing_trigger = state.db.trigger(name);
        let mut tables = Vec::new();
        let source = requested.or(existing_trigger.as_ref());
        if let Some(trigger) = source {
            for table in referenced_trigger_tables(trigger) {
                match state.db.table_identity(&table) {
                    Ok((table_id, schema_id)) => tables.push(TriggerTableBinding {
                        name: table,
                        table_id,
                        schema_id,
                    }),
                    Err(_) if state.db.external_table(&table).is_some() => {}
                    Err(_) => {
                        return Err(Box::new(error(
                            StatusCode::BAD_REQUEST,
                            "TRIGGER_VALIDATION",
                            &format!("trigger references unknown table {table:?}"),
                        )))
                    }
                }
            }
        }
        if state.db.security_version() == security_version
            && state.db.catalog_snapshot().db_epoch == catalog_epoch
        {
            return Ok(TriggerExecutionBinding {
                catalog_epoch,
                existing_trigger,
                payload: TriggerPayloadBinding {
                    security_version,
                    tables,
                },
            });
        }
    }
    Err(Box::new(error(
        StatusCode::CONFLICT,
        "TRIGGER_VALIDATION",
        "authorization or schema changed during trigger preflight",
    )))
}

fn referenced_trigger_tables(trigger: &StoredTrigger) -> Vec<String> {
    let mut names = std::collections::BTreeSet::new();
    if let TriggerTarget::Table(name) = &trigger.target {
        names.insert(name.clone());
    }
    collect_trigger_step_tables(&trigger.program.steps, &mut names);
    names.into_iter().collect()
}

fn collect_trigger_step_tables(
    steps: &[TriggerStep],
    names: &mut std::collections::BTreeSet<String>,
) {
    for step in steps {
        match step {
            TriggerStep::Insert { table, .. }
            | TriggerStep::UpdateByPk { table, .. }
            | TriggerStep::DeleteByPk { table, .. }
            | TriggerStep::Select { table, .. }
            | TriggerStep::DeleteWhere { table, .. }
            | TriggerStep::UpdateWhere { table, .. } => {
                names.insert(table.clone());
            }
            TriggerStep::Foreach { steps, .. } => collect_trigger_step_tables(steps, names),
            TriggerStep::SetNew { .. } | TriggerStep::Raise { .. } => {}
        }
    }
}

fn validate_trigger_payload_binding(
    state: &AppState,
    principal: Option<&mongreldb_core::Principal>,
    binding: &TriggerPayloadBinding,
) -> mongreldb_core::Result<()> {
    state.db.require_for(principal, &Permission::Ddl)?;
    if state.db.security_version() != binding.security_version {
        return Err(mongreldb_core::MongrelError::Conflict(
            "trigger authorization changed".into(),
        ));
    }
    for table in &binding.tables {
        if state.db.table_identity(&table.name).ok() != Some((table.table_id, table.schema_id)) {
            return Err(mongreldb_core::MongrelError::Conflict(format!(
                "trigger table {:?} changed",
                table.name
            )));
        }
    }
    Ok(())
}

fn validate_trigger_binding(
    state: &AppState,
    principal: Option<&mongreldb_core::Principal>,
    binding: &TriggerExecutionBinding,
    name: &str,
) -> mongreldb_core::Result<()> {
    validate_trigger_payload_binding(state, principal, &binding.payload)?;
    if state.db.catalog_snapshot().db_epoch != binding.catalog_epoch
        || state.db.trigger(name) != binding.existing_trigger
    {
        return Err(mongreldb_core::MongrelError::Conflict(
            "trigger catalog changed before publication".into(),
        ));
    }
    Ok(())
}

fn validate_trigger_replay(
    state: &AppState,
    principal: Option<&mongreldb_core::Principal>,
    binding: &TriggerPayloadBinding,
    name: &str,
    body: &serde_json::Value,
    dropped: bool,
) -> Result<(), Box<Response>> {
    validate_trigger_payload_binding(state, principal, binding).map_err(|failure| {
        Box::new(error(
            crate::status_for_error(&failure),
            "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            &failure.to_string(),
        ))
    })?;
    let valid = if dropped {
        body.get("dropped_trigger")
            .cloned()
            .and_then(|value| serde_json::from_value::<StoredTrigger>(value).ok())
            .is_some_and(|trigger| trigger.name == name)
            && state.db.trigger(name).is_none()
    } else {
        let expected = body
            .get("trigger")
            .cloned()
            .and_then(|value| serde_json::from_value::<StoredTrigger>(value).ok());
        expected.is_some() && state.db.trigger(name) == expected
    };
    if valid {
        Ok(())
    } else {
        Err(Box::new(error(
            StatusCode::CONFLICT,
            "IDEMPOTENCY_KEY_REUSE_MISMATCH",
            "trigger resource changed since the stored receipt",
        )))
    }
}

fn require_ddl(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
) -> Result<(), Box<Response>> {
    state
        .db
        .require_for(
            request_principal(state, principal).as_ref(),
            &mongreldb_core::Permission::Ddl,
        )
        .map_err(|error| {
            Box::new((crate::status_for_error(&error), error.to_string()).into_response())
        })
}

fn error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "status": "aborted",
            "error": {
                "code": code,
                "message": message
            }
        })),
    )
        .into_response()
}

fn request_idempotency_key(
    headers: &HeaderMap,
    body_key: Option<&str>,
) -> Result<Option<String>, Box<Response>> {
    let header_key = match headers.get("idempotency-key") {
        Some(value) => Some(value.to_str().map_err(|_| {
            Box::new(error(
                StatusCode::BAD_REQUEST,
                "INVALID_IDEMPOTENCY_KEY",
                "Idempotency-Key must be valid UTF-8",
            ))
        })?),
        None => None,
    };
    let key = match (body_key, header_key) {
        (Some(body), Some(header)) if body != header => {
            return Err(Box::new(error(
                StatusCode::BAD_REQUEST,
                "INVALID_IDEMPOTENCY_KEY",
                "body idempotency_key and Idempotency-Key header must match",
            )));
        }
        (Some(body), _) => Some(body),
        (None, Some(header)) => Some(header),
        (None, None) => None,
    };
    if let Some(key) = key {
        crate::sql_idempotency::SqlIdempotencyStore::validate_key(key).map_err(|message| {
            Box::new(error(
                StatusCode::BAD_REQUEST,
                "INVALID_IDEMPOTENCY_KEY",
                message,
            ))
        })?;
    }
    Ok(key.map(ToOwned::to_owned))
}
