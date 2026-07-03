use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use mongreldb_core::{StoredTrigger, TriggerDefinition};
use serde::Deserialize;
use serde_json::{json, Value as Jval};

use crate::AppState;

#[derive(Deserialize)]
pub struct TriggerRequest {
    trigger: StoredTrigger,
    #[serde(default)]
    idempotency_key: Option<String>,
}

pub async fn list(State(state): State<Arc<AppState>>) -> Response {
    Json(json!({ "triggers": state.db.triggers() })).into_response()
}

pub async fn describe(State(state): State<Arc<AppState>>, Path(name): Path<String>) -> Response {
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
    Json(req): Json<TriggerRequest>,
) -> Response {
    let key = request_idempotency_key(&headers, req.idempotency_key.as_deref());
    idempotent_json(&state, "trigger:create", key, || {
        normalized(req.trigger)
            .and_then(|trigger| state.db.create_trigger(trigger))
            .map(|trigger| json!({ "status": "ok", "trigger": trigger }))
            .map_err(|e| {
                Box::new(error(
                    StatusCode::BAD_REQUEST,
                    "TRIGGER_VALIDATION",
                    &e.to_string(),
                ))
            })
    })
}

pub async fn replace(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(req): Json<TriggerRequest>,
) -> Response {
    let mut trigger = req.trigger;
    trigger.name = name;
    let key = request_idempotency_key(&headers, req.idempotency_key.as_deref());
    idempotent_json(&state, "trigger:replace", key, || {
        normalized(trigger)
            .and_then(|trigger| state.db.create_or_replace_trigger(trigger))
            .map(|trigger| json!({ "status": "ok", "trigger": trigger }))
            .map_err(|e| {
                Box::new(error(
                    StatusCode::BAD_REQUEST,
                    "TRIGGER_VALIDATION",
                    &e.to_string(),
                ))
            })
    })
}

pub async fn drop_trigger(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    let key = request_idempotency_key(&headers, None);
    idempotent_json(&state, "trigger:drop", key, || {
        state
            .db
            .drop_trigger(&name)
            .map(|()| json!({ "status": "ok" }))
            .map_err(|e| Box::new(error(StatusCode::NOT_FOUND, "TRIGGER_NOT_FOUND", &e.to_string())))
    })
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

fn request_idempotency_key(headers: &HeaderMap, body_key: Option<&str>) -> Option<String> {
    body_key
        .filter(|key| !key.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            headers
                .get("idempotency-key")
                .or_else(|| headers.get("Idempotency-Key"))
                .and_then(|value| value.to_str().ok())
                .filter(|key| !key.is_empty())
                .map(ToOwned::to_owned)
        })
}

fn idempotent_json(
    state: &Arc<AppState>,
    scope: &str,
    key: Option<String>,
    f: impl FnOnce() -> Result<Jval, Box<Response>>,
) -> Response {
    let Some(key) = key else {
        return match f() {
            Ok(value) => Json(value).into_response(),
            Err(response) => *response,
        };
    };
    let scoped_key = format!("{scope}:{key}");
    if let Some(cached) = state.idem.get_json(&scoped_key) {
        return Json(cached).into_response();
    }
    let lock = state.idem.key_lock(&scoped_key);
    let _guard = lock.lock().unwrap();
    if let Some(cached) = state.idem.get_json(&scoped_key) {
        return Json(cached).into_response();
    }
    match f() {
        Ok(value) => {
            state.idem.store_json(scoped_key, value.clone());
            Json(value).into_response()
        }
        Err(response) => *response,
    }
}
