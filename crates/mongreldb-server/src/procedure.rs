use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_core::procedure::{ProcedureCallOutput, ProcedureCallRow, StoredProcedure};
use mongreldb_core::Value;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{request_principal, AppState, OptionalPrincipal};

#[derive(Deserialize)]
pub struct ProcedureRequest {
    procedure: StoredProcedure,
}

#[derive(Deserialize, Serialize)]
pub struct CallRequest {
    #[serde(default)]
    args: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
struct ProcedureCallIdempotencyPayload<'a> {
    args: &'a serde_json::Map<String, serde_json::Value>,
    procedure: ProcedureRevision<'a>,
    security_version: u64,
}

#[derive(Serialize)]
struct ProcedureRevision<'a> {
    name: &'a str,
    created_epoch: u64,
    updated_epoch: u64,
    version: u64,
    checksum: &'a str,
}

pub async fn list(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
) -> Response {
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    Json(json!({ "procedures": state.db().procedures() })).into_response()
}

pub async fn describe(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    match state.db().procedure(&name) {
        Some(procedure) => Json(json!({ "procedure": procedure })).into_response(),
        None => error(
            StatusCode::NOT_FOUND,
            "PROCEDURE_NOT_FOUND",
            "procedure not found",
        ),
    }
}

pub async fn create(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Json(req): Json<ProcedureRequest>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    match normalized(req.procedure).and_then(|procedure| state.db().create_procedure(procedure)) {
        Ok(procedure) => Json(json!({ "status": "ok", "procedure": procedure })).into_response(),
        Err(failure) => crate::kit::durable_core_error_response(&failure).unwrap_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "PROCEDURE_VALIDATION",
                &failure.to_string(),
            )
        }),
    }
}

pub async fn replace(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
    Json(req): Json<ProcedureRequest>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    let mut procedure = req.procedure;
    procedure.name = name;
    match normalized(procedure)
        .and_then(|procedure| state.db().create_or_replace_procedure(procedure))
    {
        Ok(procedure) => Json(json!({ "status": "ok", "procedure": procedure })).into_response(),
        Err(failure) => crate::kit::durable_core_error_response(&failure).unwrap_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                "PROCEDURE_VALIDATION",
                &failure.to_string(),
            )
        }),
    }
}

pub async fn drop_procedure(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    if let Err(response) = require_ddl(&state, &principal) {
        return *response;
    }
    match state.db().drop_procedure_with_epoch(&name) {
        Ok(epoch) => Json(json!({
            "status": "committed",
            "epoch": epoch.0,
            "epoch_text": epoch.0.to_string()
        }))
        .into_response(),
        Err(failure) => crate::kit::durable_core_error_response(&failure).unwrap_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                "PROCEDURE_NOT_FOUND",
                &failure.to_string(),
            )
        }),
    }
}

pub async fn call(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
    Json(req): Json<CallRequest>,
) -> Response {
    call_inner(state.clone(), name, req, principal).await
}

pub async fn kit_call(
    State(state): State<Arc<AppState>>,
    OptionalPrincipal(principal): OptionalPrincipal,
    Path(name): Path<String>,
    Json(req): Json<CallRequest>,
) -> Response {
    call_inner(state.clone(), name, req, principal).await
}

async fn call_inner(
    state: Arc<AppState>,
    name: String,
    req: CallRequest,
    authenticated_user: Option<mongreldb_core::Principal>,
) -> Response {
    if let Some(response) = crate::require_writes_open(&state) {
        return response;
    }
    let principal = request_principal(&state, &authenticated_user);
    let (procedure, security_version) =
        match authorized_procedure_revision(&state, &name, principal.as_ref()) {
            Ok(preflight) => preflight,
            Err(response) => return *response,
        };
    let owner = match crate::kit::idempotency_owner(&state, authenticated_user.as_ref()) {
        Ok(owner) => owner,
        Err(response) => return *response,
    };
    let operation = format!("procedure:call:{name}");
    let payload = ProcedureCallIdempotencyPayload {
        args: &req.args,
        procedure: ProcedureRevision {
            name: &procedure.name,
            created_epoch: procedure.created_epoch,
            updated_epoch: procedure.updated_epoch,
            version: procedure.version,
            checksum: &procedure.checksum,
        },
        security_version,
    };
    crate::kit::idempotent_json(
        &state,
        &owner,
        &operation,
        req.idempotency_key.as_deref(),
        &payload,
        || {
            let args = req
                .args
                .iter()
                .map(|(key, value)| Ok((key.clone(), json_value_to_core(value)?)))
                .collect::<Result<HashMap<_, _>, String>>()
                .map_err(|message| {
                    crate::kit::IdempotentJsonFailure::safe(error(
                        StatusCode::BAD_REQUEST,
                        "PROCEDURE_VALIDATION",
                        &message,
                    ))
                })?;
            match state
                .db()
                .call_procedure_as_bound(&procedure, args, principal.as_ref())
            {
                Ok(result) => Ok(json!({
                    "status": "ok",
                    "committed": result.epoch.is_some(),
                    "epoch": result.epoch,
                    "epoch_text": result.epoch.map(|epoch| epoch.to_string()),
                    "result": output_json(&result.output),
                })),
                Err(error @ mongreldb_core::MongrelError::NotFound(_)) => {
                    Err(crate::kit::idempotent_core_failure(
                        error,
                        StatusCode::NOT_FOUND,
                        "PROCEDURE_NOT_FOUND",
                    ))
                }
                Err(error) => Err(crate::kit::idempotent_core_failure(
                    error,
                    StatusCode::BAD_REQUEST,
                    "PROCEDURE_EXECUTION",
                )),
            }
        },
    )
    .await
}

fn authorized_procedure_revision(
    state: &AppState,
    name: &str,
    principal: Option<&mongreldb_core::Principal>,
) -> Result<(StoredProcedure, u64), Box<Response>> {
    for _ in 0..3 {
        let security_version = state.db().security_version();
        if let Err(failure) = state
            .db()
            .require_for(principal, &mongreldb_core::Permission::All)
        {
            return Err(Box::new(error(
                crate::status_for_error(&failure),
                "PROCEDURE_EXECUTION",
                &failure.to_string(),
            )));
        }
        let Some(procedure) = state.db().procedure(name) else {
            return Err(Box::new(error(
                StatusCode::NOT_FOUND,
                "PROCEDURE_NOT_FOUND",
                "procedure not found",
            )));
        };
        if state.db().security_version() == security_version {
            return Ok((procedure, security_version));
        }
    }
    Err(Box::new(error(
        StatusCode::CONFLICT,
        "PROCEDURE_EXECUTION",
        "authorization changed during procedure preflight",
    )))
}

fn require_ddl(
    state: &AppState,
    principal: &Option<mongreldb_core::Principal>,
) -> Result<(), Box<Response>> {
    state
        .db()
        .require_for(
            request_principal(state, principal).as_ref(),
            &mongreldb_core::Permission::Ddl,
        )
        .map_err(|error| {
            Box::new((crate::status_for_error(&error), error.to_string()).into_response())
        })
}

fn normalized(procedure: StoredProcedure) -> mongreldb_core::Result<StoredProcedure> {
    StoredProcedure::new(
        procedure.name,
        procedure.mode,
        procedure.params,
        procedure.body,
        0,
    )
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

fn json_value_to_core(value: &serde_json::Value) -> Result<Value, String> {
    match value {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(value) => Ok(Value::Bool(*value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(Value::Int64(value))
            } else if let Some(value) = value.as_f64() {
                Ok(Value::Float64(value))
            } else {
                Err("unsupported JSON number".into())
            }
        }
        serde_json::Value::String(value) => Ok(Value::Bytes(value.as_bytes().to_vec())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err("procedure args only support scalar JSON values".into())
        }
    }
}

fn output_json(output: &ProcedureCallOutput) -> serde_json::Value {
    match output {
        ProcedureCallOutput::Null => serde_json::Value::Null,
        ProcedureCallOutput::Scalar(value) => core_value_json(value),
        ProcedureCallOutput::Row(row) => row_json(row),
        ProcedureCallOutput::Rows(rows) => {
            serde_json::Value::Array(rows.iter().map(row_json).collect())
        }
        ProcedureCallOutput::Object(fields) => serde_json::Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), output_json(value)))
                .collect(),
        ),
        ProcedureCallOutput::Array(values) => {
            serde_json::Value::Array(values.iter().map(output_json).collect())
        }
    }
}

fn row_json(row: &ProcedureCallRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    if let Some(row_id) = row.row_id {
        obj.insert(
            "row_id".into(),
            serde_json::Value::String(row_id.0.to_string()),
        );
    }
    obj.insert(
        "columns".into(),
        serde_json::Value::Object(
            row.columns
                .iter()
                .map(|(id, value)| (id.to_string(), core_value_json(value)))
                .collect(),
        ),
    );
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
            .unwrap_or_else(|_| serde_json::Value::Array(value.iter().map(|b| json!(b)).collect())),
        Value::Embedding(values) => serde_json::Value::Array(
            values
                .iter()
                .map(|v| {
                    serde_json::Number::from_f64(*v as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::GeneratedEmbedding(value) => serde_json::Value::Array(
            value
                .vector
                .iter()
                .map(|v| {
                    serde_json::Number::from_f64(*v as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Uuid(_) | Value::Json(_) => serde_json::Value::Null,
        Value::Interval {
            months,
            days,
            nanos,
        } => serde_json::json!({"months": months, "days": days, "nanos": nanos}),
    }
}
