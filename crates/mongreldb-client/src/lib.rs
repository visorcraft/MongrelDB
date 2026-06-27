//! mongreldb-client — a lightweight HTTP client for `mongreldb-server`.
//! Table-qualified routes for multi-table operations + SQL + /txn batch.

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
        Ok(self
            .client
            .get(format!("{}/health", self.base_url))
            .send()?
            .text()?)
    }

    // ── Table management ──

    pub fn list_tables(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(self
            .client
            .get(format!("{}/tables", self.base_url))
            .send()?
            .json()?)
    }

    pub fn create_table(
        &self,
        name: &str,
        columns: Vec<ColumnDefJson>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let resp: serde_json::Value = self
            .client
            .post(format!("{}/tables", self.base_url))
            .json(&serde_json::json!({ "name": name, "columns": columns }))
            .send()?
            .json()?;
        Ok(resp["table_id"].as_u64().unwrap_or(0))
    }

    pub fn drop_table(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .delete(format!("{}/tables/{name}", self.base_url))
            .send()?;
        Ok(())
    }

    // ── Table-qualified operations ──

    pub fn count(&self, table: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let resp: CountResp = self
            .client
            .get(format!("{}/tables/{table}/count", self.base_url))
            .send()?
            .json()?;
        Ok(resp.count)
    }

    pub fn put(
        &self,
        table: &str,
        row: Vec<(u16, mongreldb_core::Value)>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let json_row: Vec<serde_json::Value> = row
            .iter()
            .flat_map(|(id, v)| vec![serde_json::json!(id), value_to_json(v)])
            .collect();
        let resp: serde_json::Value = self
            .client
            .post(format!("{}/tables/{table}/put", self.base_url))
            .json(&serde_json::json!({ "row": json_row }))
            .send()?
            .json()?;
        Ok(resp["row_id"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0))
    }

    pub fn commit(&self, table: &str) -> Result<u64, Box<dyn std::error::Error>> {
        let resp: serde_json::Value = self
            .client
            .post(format!("{}/tables/{table}/commit", self.base_url))
            .send()?
            .json()?;
        Ok(resp["epoch"].as_u64().unwrap_or(0))
    }

    // ── SQL ──

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

    // ── Atomic txn ──

    pub fn txn(&self, ops: Vec<TxnOp>) -> Result<(), Box<dyn std::error::Error>> {
        self.client
            .post(format!("{}/txn", self.base_url))
            .json(&serde_json::json!({ "ops": ops }))
            .send()?;
        Ok(())
    }
}

#[derive(Serialize, Clone)]
pub struct ColumnDefJson {
    pub id: u16,
    pub name: String,
    pub ty: String,
    pub primary_key: bool,
}

#[derive(Serialize, Clone)]
pub struct TxnOp {
    pub table: String,
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cells: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_id: Option<u64>,
}

fn value_to_json(v: &mongreldb_core::Value) -> serde_json::Value {
    match v {
        mongreldb_core::Value::Int64(n) => serde_json::Value::Number((*n).into()),
        mongreldb_core::Value::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        mongreldb_core::Value::Bytes(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        mongreldb_core::Value::Bool(b) => serde_json::Value::Bool(*b),
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
