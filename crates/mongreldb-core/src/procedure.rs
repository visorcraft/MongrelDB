use crate::error::{MongrelError, Result};
use crate::rowid::RowId;
use crate::schema::TypeId;
use crate::Value;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcedureMode {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureParam {
    pub name: String,
    pub ty: TypeId,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureBody {
    pub steps: Vec<ProcedureStep>,
    pub return_value: ProcedureValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ProcedureStep {
    NativeQuery {
        id: String,
        table: String,
        #[serde(default)]
        conditions: Vec<ProcedureCondition>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        projection: Option<Vec<u16>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        limit: Option<usize>,
    },
    Put {
        id: String,
        table: String,
        cells: Vec<ProcedureCell>,
        #[serde(default)]
        returning: bool,
    },
    Upsert {
        id: String,
        table: String,
        cells: Vec<ProcedureCell>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        update_cells: Option<Vec<ProcedureCell>>,
        #[serde(default)]
        returning: bool,
    },
    DeleteByPk {
        id: String,
        table: String,
        pk: ProcedureValue,
    },
    DeleteRows {
        id: String,
        table: String,
        row_ids: ProcedureValue,
    },
    SqlQuery {
        id: String,
        sql: String,
        #[serde(default)]
        params: Vec<ProcedureValue>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureCell {
    pub column_id: u16,
    pub value: ProcedureValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ProcedureCondition {
    Pk {
        value: ProcedureValue,
    },
    BitmapEq {
        column_id: u16,
        value: ProcedureValue,
    },
    BitmapIn {
        column_id: u16,
        values: Vec<ProcedureValue>,
    },
    Range {
        column_id: u16,
        lo: ProcedureValue,
        hi: ProcedureValue,
    },
    RangeF64 {
        column_id: u16,
        lo: ProcedureValue,
        lo_inclusive: bool,
        hi: ProcedureValue,
        hi_inclusive: bool,
    },
    IsNull {
        column_id: u16,
    },
    IsNotNull {
        column_id: u16,
    },
    FmContains {
        column_id: u16,
        pattern: ProcedureValue,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ProcedureValue {
    Literal(Value),
    Param(String),
    StepRows(String),
    StepRow(String),
    StepScalar(String),
    Object(Vec<(String, ProcedureValue)>),
    Array(Vec<ProcedureValue>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredProcedure {
    pub name: String,
    pub version: u64,
    pub mode: ProcedureMode,
    pub params: Vec<ProcedureParam>,
    pub body: ProcedureBody,
    pub checksum: String,
    pub created_epoch: u64,
    pub updated_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureEntry {
    pub procedure: StoredProcedure,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureCallResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    pub output: ProcedureCallOutput,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ProcedureCallOutput {
    Null,
    Scalar(Value),
    Row(ProcedureCallRow),
    Rows(Vec<ProcedureCallRow>),
    Object(Vec<(String, ProcedureCallOutput)>),
    Array(Vec<ProcedureCallOutput>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureCallRow {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_id: Option<RowId>,
    pub columns: HashMap<u16, Value>,
}

impl StoredProcedure {
    pub fn new(
        name: impl Into<String>,
        mode: ProcedureMode,
        params: Vec<ProcedureParam>,
        body: ProcedureBody,
        epoch: u64,
    ) -> Result<Self> {
        let name = name.into();
        let checksum = checksum(&name, 1, &mode, &params, &body)?;
        let proc = Self {
            name,
            version: 1,
            mode,
            params,
            body,
            checksum,
            created_epoch: epoch,
            updated_epoch: epoch,
        };
        proc.validate()?;
        Ok(proc)
    }

    pub fn replaced(&self, mut next: StoredProcedure, epoch: u64) -> Result<StoredProcedure> {
        next.name = self.name.clone();
        next.version = self.version + 1;
        next.created_epoch = self.created_epoch;
        next.updated_epoch = epoch;
        next.checksum = checksum(
            &next.name,
            next.version,
            &next.mode,
            &next.params,
            &next.body,
        )?;
        next.validate()?;
        Ok(next)
    }

    pub fn validate(&self) -> Result<()> {
        validate_name("procedure", &self.name)?;
        let mut params = HashSet::new();
        for param in &self.params {
            validate_name("procedure parameter", &param.name)?;
            if !params.insert(param.name.as_str()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate procedure parameter {:?}",
                    param.name
                )));
            }
            if !param.nullable && matches!(param.default, Some(Value::Null)) {
                return Err(MongrelError::InvalidArgument(format!(
                    "non-null procedure parameter {:?} has NULL default",
                    param.name
                )));
            }
        }

        let mut steps = HashSet::new();
        for step in &self.body.steps {
            let id = step.id();
            validate_name("procedure step", id)?;
            if !steps.insert(id) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate procedure step {id:?}"
                )));
            }
            if self.mode == ProcedureMode::ReadOnly && step.is_write() {
                return Err(MongrelError::InvalidArgument(format!(
                    "read-only procedure {:?} contains write step {id:?}",
                    self.name
                )));
            }
        }
        validate_value_refs(&self.body.return_value, &params, &steps)
    }
}

impl ProcedureStep {
    pub fn id(&self) -> &str {
        match self {
            ProcedureStep::NativeQuery { id, .. }
            | ProcedureStep::Put { id, .. }
            | ProcedureStep::Upsert { id, .. }
            | ProcedureStep::DeleteByPk { id, .. }
            | ProcedureStep::DeleteRows { id, .. }
            | ProcedureStep::SqlQuery { id, .. } => id,
        }
    }

    pub fn table(&self) -> Option<&str> {
        match self {
            ProcedureStep::NativeQuery { table, .. }
            | ProcedureStep::Put { table, .. }
            | ProcedureStep::Upsert { table, .. }
            | ProcedureStep::DeleteByPk { table, .. }
            | ProcedureStep::DeleteRows { table, .. } => Some(table),
            ProcedureStep::SqlQuery { .. } => None,
        }
    }

    pub fn is_write(&self) -> bool {
        matches!(
            self,
            ProcedureStep::Put { .. }
                | ProcedureStep::Upsert { .. }
                | ProcedureStep::DeleteByPk { .. }
                | ProcedureStep::DeleteRows { .. }
        )
    }
}

impl From<StoredProcedure> for ProcedureEntry {
    fn from(procedure: StoredProcedure) -> Self {
        Self { procedure }
    }
}

#[derive(Serialize)]
struct ChecksumInput<'a> {
    name: &'a str,
    version: u64,
    mode: &'a ProcedureMode,
    params: &'a [ProcedureParam],
    body: &'a ProcedureBody,
}

fn checksum(
    name: &str,
    version: u64,
    mode: &ProcedureMode,
    params: &[ProcedureParam],
    body: &ProcedureBody,
) -> Result<String> {
    let input = ChecksumInput {
        name,
        version,
        mode,
        params,
        body,
    };
    let bytes = serde_json::to_vec(&input)
        .map_err(|e| MongrelError::Other(format!("procedure checksum serialize: {e}")))?;
    Ok(hex(&Sha256::digest(bytes)))
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn validate_name(kind: &str, name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(MongrelError::InvalidArgument(format!(
            "{kind} name {name:?} must be non-empty ASCII alphanumeric, '_' or '-'"
        )));
    }
    Ok(())
}

fn validate_value_refs(
    value: &ProcedureValue,
    params: &HashSet<&str>,
    steps: &HashSet<&str>,
) -> Result<()> {
    match value {
        ProcedureValue::Literal(_) => Ok(()),
        ProcedureValue::Param(name) => {
            if params.contains(name.as_str()) {
                Ok(())
            } else {
                Err(MongrelError::InvalidArgument(format!(
                    "unknown procedure parameter reference {name:?}"
                )))
            }
        }
        ProcedureValue::StepRows(id)
        | ProcedureValue::StepRow(id)
        | ProcedureValue::StepScalar(id) => {
            if steps.contains(id.as_str()) {
                Ok(())
            } else {
                Err(MongrelError::InvalidArgument(format!(
                    "unknown procedure step reference {id:?}"
                )))
            }
        }
        ProcedureValue::Object(fields) => {
            for (_, v) in fields {
                validate_value_refs(v, params, steps)?;
            }
            Ok(())
        }
        ProcedureValue::Array(values) => {
            for v in values {
                validate_value_refs(v, params, steps)?;
            }
            Ok(())
        }
    }
}
