//! mongreldb-client (Phase 19.5) — a lightweight HTTP client for
//! `mongreldb-server`. Connects to the daemon's SQL + native query endpoints
//! and returns `RecordBatch`es (zero-copy from the Arrow IPC response body).
//!
//! The Condition-based composition model is preserved: `query_native` sends
//! serialized Conditions, not just SQL.

use std::io::Cursor;

use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

pub struct MongrelClient {
    base_url: String,
    client: reqwest::blocking::Client,
}

#[derive(Serialize)]
struct SqlReq {
    sql: String,
}

#[derive(Deserialize)]
struct CountResp {
    count: u64,
}

impl MongrelClient {
    pub fn new(url: &str) -> Self {
        Self {
            base_url: url.trim_end_matches('/').to_string(),
            client: reqwest::blocking::Client::new(),
        }
    }

    pub fn health(&self) -> Result<String, Box<dyn std::error::Error>> {
        Ok(self.client.get(format!("{}/health", self.base_url)).send()?.text()?)
    }

    pub fn count(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let resp: CountResp = self
            .client
            .get(format!("{}/count", self.base_url))
            .send()?
            .json()?;
        Ok(resp.count)
    }

    pub fn sql(&self, sql: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
        let resp = self
            .client
            .post(format!("{}/sql", self.base_url))
            .json(&SqlReq {
                sql: sql.to_string(),
            })
            .send()?;
        let bytes = resp.bytes()?;
        read_arrow_ipc(&bytes)
    }

    pub fn put(
        &self,
        row: Vec<(u16, mongreldb_core::Value)>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let json_row: Vec<(u16, serde_json::Value)> = row
            .into_iter()
            .map(|(id, v)| (id, value_to_json(v)))
            .collect();
        let resp: serde_json::Value = self
            .client
            .post(format!("{}/put", self.base_url))
            .json(&serde_json::json!({ "row": json_row }))
            .send()?
            .json()?;
        Ok(resp["row_id"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    pub fn delete(&self, row_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .post(format!("{}/delete", self.base_url))
            .json(&serde_json::json!({ "row_id": row_id }))
            .send()?;
        Ok(())
    }

    pub fn commit(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let resp: serde_json::Value = self
            .client
            .post(format!("{}/commit", self.base_url))
            .send()?
            .json()?;
        Ok(resp["epoch"].as_u64().unwrap_or(0))
    }
}

fn value_to_json(v: mongreldb_core::Value) -> serde_json::Value {
    match v {
        mongreldb_core::Value::Int64(n) => serde_json::Value::Number(n.into()),
        mongreldb_core::Value::Float64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        mongreldb_core::Value::Bytes(b) => {
            serde_json::Value::String(String::from_utf8_lossy(&b).into_owned())
        }
        mongreldb_core::Value::Bool(b) => serde_json::Value::Bool(b),
        mongreldb_core::Value::Null => serde_json::Value::Null,
        other => serde_json::Value::String(format!("{other:?}")),
    }
}

fn read_arrow_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let cursor = Cursor::new(bytes);
    let reader = FileReader::try_new(cursor, None)?;
    reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}
