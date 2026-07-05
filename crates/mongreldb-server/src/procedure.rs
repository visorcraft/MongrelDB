use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_core::procedure::{ProcedureCallOutput, ProcedureCallRow, StoredProcedure};
use mongreldb_core::Value;
use serde::Deserialize;
use serde_json::json;

use crate::AppState;

#[derive(Deserialize)]
pub struct ProcedureRequest {
    procedure: StoredProcedure,
}

#[derive(Deserialize)]
pub struct CallRequest {
    #[serde(default)]
    args: serde_json::Map<String, serde_json::Value>,
    #[allow(dead_code)]
    #[serde(default)]
    idempotency_key: Option<String>,
}

pub async fn list(State(state): State<Arc<AppState>>) -> Response {
    Json(json!({ "procedures": state.db.procedures() })).into_response()
}

pub async fn describe(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
    match state.db.procedure(&name) {
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
    Json(req): Json<ProcedureRequest>,
) -> Response {
    match normalized(req.procedure).and_then(|procedure| state.db.create_procedure(procedure)) {
        Ok(procedure) => Json(json!({ "status": "ok", "procedure": procedure })).into_response(),
        Err(e) => error(
            StatusCode::BAD_REQUEST,
            "PROCEDURE_VALIDATION",
            &e.to_string(),
        ),
    }
}

pub async fn replace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<ProcedureRequest>,
) -> Response {
    let mut procedure = req.procedure;
    procedure.name = name;
    match normalized(procedure)
        .and_then(|procedure| state.db.create_or_replace_procedure(procedure))
    {
        Ok(procedure) => Json(json!({ "status": "ok", "procedure": procedure })).into_response(),
        Err(e) => error(
            StatusCode::BAD_REQUEST,
            "PROCEDURE_VALIDATION",
            &e.to_string(),
        ),
    }
}

pub async fn drop_procedure(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    match state.db.drop_procedure(&name) {
        Ok(()) => Json(json!({ "status": "ok" })).into_response(),
        Err(e) => error(StatusCode::NOT_FOUND, "PROCEDURE_NOT_FOUND", &e.to_string()),
    }
}

pub async fn call(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<CallRequest>,
) -> Response {
    call_inner(state, name, req)
}

pub async fn kit_call(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<CallRequest>,
) -> Response {
    call_inner(state, name, req)
}

fn call_inner(state: Arc<AppState>, name: String, req: CallRequest) -> Response {
    let args = match req
        .args
        .iter()
        .map(|(key, value)| Ok((key.clone(), json_value_to_core(value)?)))
        .collect::<Result<HashMap<_, _>, String>>()
    {
        Ok(args) => args,
        Err(e) => return error(StatusCode::BAD_REQUEST, "PROCEDURE_VALIDATION", &e),
    };
    match state.db.call_procedure(&name, args) {
        Ok(result) => Json(json!({
            "status": "ok",
            "epoch": result.epoch,
            "result": output_json(&result.output),
        }))
        .into_response(),
        Err(mongreldb_core::MongrelError::NotFound(e)) => {
            error(StatusCode::NOT_FOUND, "PROCEDURE_NOT_FOUND", &e)
        }
        Err(e) => error(
            StatusCode::BAD_REQUEST,
            "PROCEDURE_EXECUTION",
            &e.to_string(),
        ),
    }
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
        Value::Decimal(d) => serde_json::Value::String(d.to_string()),
        Value::Interval { months, days, nanos } => serde_json::json!({"months": months, "days": days, "nanos": nanos}),
    }
}
