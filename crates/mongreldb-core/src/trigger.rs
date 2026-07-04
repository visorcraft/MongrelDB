use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use crate::schema::ColumnDef;
use serde::de::{self, Deserializer};
use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerEntry {
    pub trigger: StoredTrigger,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredTrigger {
    pub name: String,
    pub version: u64,
    pub target: TriggerTarget,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    #[serde(default)]
    pub update_of: Vec<String>,
    #[serde(default)]
    pub target_columns: Vec<ColumnDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<TriggerExpr>,
    pub program: TriggerProgram,
    pub enabled: bool,
    pub checksum: String,
    pub created_epoch: u64,
    pub updated_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerDefinition {
    pub target: TriggerTarget,
    pub timing: TriggerTiming,
    pub event: TriggerEvent,
    #[serde(default)]
    pub update_of: Vec<String>,
    #[serde(default)]
    pub target_columns: Vec<ColumnDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<TriggerExpr>,
    pub program: TriggerProgram,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerConfig {
    pub recursive_triggers: bool,
    pub max_depth: u32,
    #[serde(default)]
    pub max_loop_iterations: u32,
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            recursive_triggers: false,
            max_depth: 32,
            max_loop_iterations: 10_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "name")]
pub enum TriggerTarget {
    Table(String),
    View(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TriggerProgram {
    pub steps: Vec<TriggerStep>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerStep {
    SetNew {
        cells: Vec<TriggerCell>,
    },
    Insert {
        table: String,
        cells: Vec<TriggerCell>,
    },
    UpdateByPk {
        table: String,
        pk: TriggerValue,
        cells: Vec<TriggerCell>,
    },
    DeleteByPk {
        table: String,
        pk: TriggerValue,
    },
    Select {
        id: String,
        table: String,
        conditions: Vec<TriggerCondition>,
    },
    Foreach {
        id: String,
        steps: Vec<TriggerStep>,
    },
    DeleteWhere {
        table: String,
        conditions: Vec<TriggerCondition>,
    },
    UpdateWhere {
        table: String,
        conditions: Vec<TriggerCondition>,
        cells: Vec<TriggerCell>,
    },
    Raise {
        action: TriggerRaiseAction,
        message: TriggerValue,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerCell {
    pub column_id: u16,
    pub value: TriggerValue,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerCondition {
    Pk {
        value: TriggerValue,
    },
    Eq {
        column_id: u16,
        value: TriggerValue,
    },
    NotEq {
        column_id: u16,
        value: TriggerValue,
    },
    Lt {
        column_id: u16,
        value: TriggerValue,
    },
    Lte {
        column_id: u16,
        value: TriggerValue,
    },
    Gt {
        column_id: u16,
        value: TriggerValue,
    },
    Gte {
        column_id: u16,
        value: TriggerValue,
    },
    IsNull {
        column_id: u16,
    },
    IsNotNull {
        column_id: u16,
    },
    And {
        left: Box<TriggerCondition>,
        right: Box<TriggerCondition>,
    },
    Or {
        left: Box<TriggerCondition>,
        right: Box<TriggerCondition>,
    },
    Not(Box<TriggerCondition>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum TriggerValue {
    Literal(Value),
    NewColumn(u16),
    OldColumn(u16),
    SelectedColumn(u16),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerExpr {
    Value(TriggerValue),
    Eq {
        left: TriggerValue,
        right: TriggerValue,
    },
    NotEq {
        left: TriggerValue,
        right: TriggerValue,
    },
    Lt {
        left: TriggerValue,
        right: TriggerValue,
    },
    Lte {
        left: TriggerValue,
        right: TriggerValue,
    },
    Gt {
        left: TriggerValue,
        right: TriggerValue,
    },
    Gte {
        left: TriggerValue,
        right: TriggerValue,
    },
    IsNull(TriggerValue),
    IsNotNull(TriggerValue),
    And {
        left: Box<TriggerExpr>,
        right: Box<TriggerExpr>,
    },
    Or {
        left: Box<TriggerExpr>,
        right: Box<TriggerExpr>,
    },
    Not(Box<TriggerExpr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerRaiseAction {
    Abort,
    Fail,
    Rollback,
    Ignore,
}

impl Serialize for TriggerProgram {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("steps", &self.steps)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for TriggerProgram {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| de::Error::custom("expected object"))?;
        let steps = match obj.get("steps") {
            Some(v) => parse_trigger_steps(v).map_err(de::Error::custom)?,
            None => Vec::new(),
        };
        Ok(TriggerProgram { steps })
    }
}

impl Serialize for TriggerStep {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self {
            TriggerStep::SetNew { cells } => {
                map.serialize_entry("kind", "set_new")?;
                map.serialize_entry("cells", cells)?;
            }
            TriggerStep::Insert { table, cells } => {
                map.serialize_entry("kind", "insert")?;
                map.serialize_entry("table", table)?;
                map.serialize_entry("cells", cells)?;
            }
            TriggerStep::UpdateByPk { table, pk, cells } => {
                map.serialize_entry("kind", "update_by_pk")?;
                map.serialize_entry("table", table)?;
                map.serialize_entry("pk", pk)?;
                map.serialize_entry("cells", cells)?;
            }
            TriggerStep::DeleteByPk { table, pk } => {
                map.serialize_entry("kind", "delete_by_pk")?;
                map.serialize_entry("table", table)?;
                map.serialize_entry("pk", pk)?;
            }
            TriggerStep::Select {
                id,
                table,
                conditions,
            } => {
                map.serialize_entry("kind", "select")?;
                map.serialize_entry("id", id)?;
                map.serialize_entry("table", table)?;
                map.serialize_entry("conditions", conditions)?;
            }
            TriggerStep::Foreach { id, steps } => {
                map.serialize_entry("kind", "foreach")?;
                map.serialize_entry("id", id)?;
                map.serialize_entry("steps", steps)?;
            }
            TriggerStep::DeleteWhere { table, conditions } => {
                map.serialize_entry("kind", "delete_where")?;
                map.serialize_entry("table", table)?;
                map.serialize_entry("conditions", conditions)?;
            }
            TriggerStep::UpdateWhere {
                table,
                conditions,
                cells,
            } => {
                map.serialize_entry("kind", "update_where")?;
                map.serialize_entry("table", table)?;
                map.serialize_entry("conditions", conditions)?;
                map.serialize_entry("cells", cells)?;
            }
            TriggerStep::Raise { action, message } => {
                map.serialize_entry("kind", "raise")?;
                map.serialize_entry("action", action)?;
                map.serialize_entry("message", message)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TriggerStep {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_trigger_step(&value).map_err(de::Error::custom)
    }
}

impl Serialize for TriggerCondition {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self {
            TriggerCondition::Pk { value } => {
                map.serialize_entry("kind", "pk")?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::Eq { column_id, value } => {
                map.serialize_entry("kind", "eq")?;
                map.serialize_entry("column_id", column_id)?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::NotEq { column_id, value } => {
                map.serialize_entry("kind", "not_eq")?;
                map.serialize_entry("column_id", column_id)?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::Lt { column_id, value } => {
                map.serialize_entry("kind", "lt")?;
                map.serialize_entry("column_id", column_id)?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::Lte { column_id, value } => {
                map.serialize_entry("kind", "lte")?;
                map.serialize_entry("column_id", column_id)?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::Gt { column_id, value } => {
                map.serialize_entry("kind", "gt")?;
                map.serialize_entry("column_id", column_id)?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::Gte { column_id, value } => {
                map.serialize_entry("kind", "gte")?;
                map.serialize_entry("column_id", column_id)?;
                map.serialize_entry("value", value)?;
            }
            TriggerCondition::IsNull { column_id } => {
                map.serialize_entry("kind", "is_null")?;
                map.serialize_entry("column_id", column_id)?;
            }
            TriggerCondition::IsNotNull { column_id } => {
                map.serialize_entry("kind", "is_not_null")?;
                map.serialize_entry("column_id", column_id)?;
            }
            TriggerCondition::And { left, right } => {
                map.serialize_entry("kind", "and")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerCondition::Or { left, right } => {
                map.serialize_entry("kind", "or")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerCondition::Not(value) => {
                map.serialize_entry("kind", "not")?;
                map.serialize_entry("value", value)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TriggerCondition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_trigger_condition(&value).map_err(de::Error::custom)
    }
}

impl Serialize for TriggerExpr {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self {
            TriggerExpr::Value(value) => {
                map.serialize_entry("kind", "value")?;
                map.serialize_entry("value", value)?;
            }
            TriggerExpr::Eq { left, right } => {
                map.serialize_entry("kind", "eq")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::NotEq { left, right } => {
                map.serialize_entry("kind", "not_eq")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::Lt { left, right } => {
                map.serialize_entry("kind", "lt")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::Lte { left, right } => {
                map.serialize_entry("kind", "lte")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::Gt { left, right } => {
                map.serialize_entry("kind", "gt")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::Gte { left, right } => {
                map.serialize_entry("kind", "gte")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::IsNull(value) => {
                map.serialize_entry("kind", "is_null")?;
                map.serialize_entry("value", value)?;
            }
            TriggerExpr::IsNotNull(value) => {
                map.serialize_entry("kind", "is_not_null")?;
                map.serialize_entry("value", value)?;
            }
            TriggerExpr::And { left, right } => {
                map.serialize_entry("kind", "and")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::Or { left, right } => {
                map.serialize_entry("kind", "or")?;
                map.serialize_entry("left", left)?;
                map.serialize_entry("right", right)?;
            }
            TriggerExpr::Not(value) => {
                map.serialize_entry("kind", "not")?;
                map.serialize_entry("value", value)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TriggerExpr {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        parse_trigger_expr(&value).map_err(de::Error::custom)
    }
}

fn parse_trigger_steps(value: &serde_json::Value) -> std::result::Result<Vec<TriggerStep>, String> {
    value
        .as_array()
        .ok_or_else(|| "expected array for steps".to_string())?
        .iter()
        .map(parse_trigger_step)
        .collect()
}

fn parse_trigger_step(value: &serde_json::Value) -> std::result::Result<TriggerStep, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "expected object".to_string())?;
    let kind = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing kind".to_string())?;
    match kind {
        "set_new" => Ok(TriggerStep::SetNew {
            cells: parse_trigger_cells(
                obj.get("cells")
                    .ok_or_else(|| "missing cells".to_string())?,
            )?,
        }),
        "insert" => Ok(TriggerStep::Insert {
            table: parse_string(
                obj.get("table")
                    .ok_or_else(|| "missing table".to_string())?,
                "table",
            )?,
            cells: parse_trigger_cells(
                obj.get("cells")
                    .ok_or_else(|| "missing cells".to_string())?,
            )?,
        }),
        "update_by_pk" => Ok(TriggerStep::UpdateByPk {
            table: parse_string(
                obj.get("table")
                    .ok_or_else(|| "missing table".to_string())?,
                "table",
            )?,
            pk: parse_trigger_value(obj.get("pk").ok_or_else(|| "missing pk".to_string())?)?,
            cells: parse_trigger_cells(
                obj.get("cells")
                    .ok_or_else(|| "missing cells".to_string())?,
            )?,
        }),
        "delete_by_pk" => Ok(TriggerStep::DeleteByPk {
            table: parse_string(
                obj.get("table")
                    .ok_or_else(|| "missing table".to_string())?,
                "table",
            )?,
            pk: parse_trigger_value(obj.get("pk").ok_or_else(|| "missing pk".to_string())?)?,
        }),
        "select" => Ok(TriggerStep::Select {
            id: parse_string(obj.get("id").ok_or_else(|| "missing id".to_string())?, "id")?,
            table: parse_string(
                obj.get("table")
                    .ok_or_else(|| "missing table".to_string())?,
                "table",
            )?,
            conditions: parse_trigger_conditions_optional(obj)?,
        }),
        "foreach" => Ok(TriggerStep::Foreach {
            id: parse_string(obj.get("id").ok_or_else(|| "missing id".to_string())?, "id")?,
            steps: parse_trigger_steps(
                obj.get("steps")
                    .ok_or_else(|| "missing steps".to_string())?,
            )?,
        }),
        "delete_where" => Ok(TriggerStep::DeleteWhere {
            table: parse_string(
                obj.get("table")
                    .ok_or_else(|| "missing table".to_string())?,
                "table",
            )?,
            conditions: parse_trigger_conditions_optional(obj)?,
        }),
        "update_where" => Ok(TriggerStep::UpdateWhere {
            table: parse_string(
                obj.get("table")
                    .ok_or_else(|| "missing table".to_string())?,
                "table",
            )?,
            conditions: parse_trigger_conditions_optional(obj)?,
            cells: parse_trigger_cells(
                obj.get("cells")
                    .ok_or_else(|| "missing cells".to_string())?,
            )?,
        }),
        "raise" => Ok(TriggerStep::Raise {
            action: serde_json::from_value::<TriggerRaiseAction>(
                obj.get("action")
                    .ok_or_else(|| "missing action".to_string())?
                    .clone(),
            )
            .map_err(|e| e.to_string())?,
            message: parse_trigger_value(
                obj.get("message")
                    .ok_or_else(|| "missing message".to_string())?,
            )?,
        }),
        _ => Err(format!("unknown TriggerStep kind: {kind}")),
    }
}

fn parse_trigger_conditions_optional(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<Vec<TriggerCondition>, String> {
    match obj.get("conditions") {
        Some(v) => parse_trigger_conditions(v),
        None => Ok(Vec::new()),
    }
}

fn parse_trigger_conditions(
    value: &serde_json::Value,
) -> std::result::Result<Vec<TriggerCondition>, String> {
    value
        .as_array()
        .ok_or_else(|| "expected array for conditions".to_string())?
        .iter()
        .map(parse_trigger_condition)
        .collect()
}

fn parse_trigger_condition(
    value: &serde_json::Value,
) -> std::result::Result<TriggerCondition, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "expected object".to_string())?;
    let kind = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing kind".to_string())?;
    match kind {
        "pk" => Ok(TriggerCondition::Pk {
            value: parse_trigger_value(
                obj.get("value")
                    .ok_or_else(|| "missing value".to_string())?,
            )?,
        }),
        "eq" | "not_eq" | "lt" | "lte" | "gt" | "gte" => parse_trigger_condition_binary(obj, kind),
        "is_null" => Ok(TriggerCondition::IsNull {
            column_id: parse_u16(
                obj.get("column_id")
                    .ok_or_else(|| "missing column_id".to_string())?,
                "column_id",
            )?,
        }),
        "is_not_null" => Ok(TriggerCondition::IsNotNull {
            column_id: parse_u16(
                obj.get("column_id")
                    .ok_or_else(|| "missing column_id".to_string())?,
                "column_id",
            )?,
        }),
        "and" => Ok(TriggerCondition::And {
            left: Box::new(parse_trigger_condition(
                obj.get("left").ok_or_else(|| "missing left".to_string())?,
            )?),
            right: Box::new(parse_trigger_condition(
                obj.get("right")
                    .ok_or_else(|| "missing right".to_string())?,
            )?),
        }),
        "or" => Ok(TriggerCondition::Or {
            left: Box::new(parse_trigger_condition(
                obj.get("left").ok_or_else(|| "missing left".to_string())?,
            )?),
            right: Box::new(parse_trigger_condition(
                obj.get("right")
                    .ok_or_else(|| "missing right".to_string())?,
            )?),
        }),
        "not" => Ok(TriggerCondition::Not(Box::new(parse_trigger_condition(
            obj.get("value")
                .ok_or_else(|| "missing value".to_string())?,
        )?))),
        _ => Err(format!("unknown TriggerCondition kind: {kind}")),
    }
}

fn parse_trigger_condition_binary(
    obj: &serde_json::Map<String, serde_json::Value>,
    kind: &str,
) -> std::result::Result<TriggerCondition, String> {
    let column_id = parse_u16(
        obj.get("column_id")
            .ok_or_else(|| "missing column_id".to_string())?,
        "column_id",
    )?;
    let value = parse_trigger_value(
        obj.get("value")
            .ok_or_else(|| "missing value".to_string())?,
    )?;
    Ok(match kind {
        "eq" => TriggerCondition::Eq { column_id, value },
        "not_eq" => TriggerCondition::NotEq { column_id, value },
        "lt" => TriggerCondition::Lt { column_id, value },
        "lte" => TriggerCondition::Lte { column_id, value },
        "gt" => TriggerCondition::Gt { column_id, value },
        "gte" => TriggerCondition::Gte { column_id, value },
        _ => return Err(format!("unexpected binary condition kind: {kind}")),
    })
}

fn parse_trigger_expr(value: &serde_json::Value) -> std::result::Result<TriggerExpr, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "expected object".to_string())?;
    let kind = obj
        .get("kind")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "missing kind".to_string())?;
    match kind {
        "value" => Ok(TriggerExpr::Value(parse_trigger_value(
            obj.get("value")
                .ok_or_else(|| "missing value".to_string())?,
        )?)),
        "eq" | "not_eq" | "lt" | "lte" | "gt" | "gte" => parse_trigger_expr_binary(obj, kind),
        "is_null" => Ok(TriggerExpr::IsNull(parse_trigger_value(
            obj.get("value")
                .ok_or_else(|| "missing value".to_string())?,
        )?)),
        "is_not_null" => Ok(TriggerExpr::IsNotNull(parse_trigger_value(
            obj.get("value")
                .ok_or_else(|| "missing value".to_string())?,
        )?)),
        "and" => Ok(TriggerExpr::And {
            left: Box::new(parse_trigger_expr(
                obj.get("left").ok_or_else(|| "missing left".to_string())?,
            )?),
            right: Box::new(parse_trigger_expr(
                obj.get("right")
                    .ok_or_else(|| "missing right".to_string())?,
            )?),
        }),
        "or" => Ok(TriggerExpr::Or {
            left: Box::new(parse_trigger_expr(
                obj.get("left").ok_or_else(|| "missing left".to_string())?,
            )?),
            right: Box::new(parse_trigger_expr(
                obj.get("right")
                    .ok_or_else(|| "missing right".to_string())?,
            )?),
        }),
        "not" => Ok(TriggerExpr::Not(Box::new(parse_trigger_expr(
            obj.get("value")
                .ok_or_else(|| "missing value".to_string())?,
        )?))),
        _ => Err(format!("unknown TriggerExpr kind: {kind}")),
    }
}

fn parse_trigger_expr_binary(
    obj: &serde_json::Map<String, serde_json::Value>,
    kind: &str,
) -> std::result::Result<TriggerExpr, String> {
    let left = parse_trigger_value(obj.get("left").ok_or_else(|| "missing left".to_string())?)?;
    let right = parse_trigger_value(
        obj.get("right")
            .ok_or_else(|| "missing right".to_string())?,
    )?;
    Ok(match kind {
        "eq" => TriggerExpr::Eq { left, right },
        "not_eq" => TriggerExpr::NotEq { left, right },
        "lt" => TriggerExpr::Lt { left, right },
        "lte" => TriggerExpr::Lte { left, right },
        "gt" => TriggerExpr::Gt { left, right },
        "gte" => TriggerExpr::Gte { left, right },
        _ => return Err(format!("unexpected binary expr kind: {kind}")),
    })
}

fn parse_trigger_value(value: &serde_json::Value) -> std::result::Result<TriggerValue, String> {
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}

fn parse_trigger_cells(value: &serde_json::Value) -> std::result::Result<Vec<TriggerCell>, String> {
    value
        .as_array()
        .ok_or_else(|| "expected array for cells".to_string())?
        .iter()
        .map(parse_trigger_cell)
        .collect()
}

fn parse_trigger_cell(value: &serde_json::Value) -> std::result::Result<TriggerCell, String> {
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}

fn parse_u16(value: &serde_json::Value, field: &str) -> std::result::Result<u16, String> {
    value
        .as_u64()
        .and_then(|n| u16::try_from(n).ok())
        .ok_or_else(|| format!("expected u16 for {field}"))
}

fn parse_string(value: &serde_json::Value, field: &str) -> std::result::Result<String, String> {
    value
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| format!("expected string for {field}"))
}

impl StoredTrigger {
    pub fn new(name: impl Into<String>, definition: TriggerDefinition, epoch: u64) -> Result<Self> {
        let name = name.into();
        let checksum = checksum(ChecksumInput {
            name: &name,
            version: 1,
            target: &definition.target,
            timing: definition.timing,
            event: definition.event,
            update_of: &definition.update_of,
            target_columns: &definition.target_columns,
            when: &definition.when,
            program: &definition.program,
        })?;
        let trigger = Self {
            name,
            version: 1,
            target: definition.target,
            timing: definition.timing,
            event: definition.event,
            update_of: definition.update_of,
            target_columns: definition.target_columns,
            when: definition.when,
            program: definition.program,
            enabled: true,
            checksum,
            created_epoch: epoch,
            updated_epoch: epoch,
        };
        trigger.validate()?;
        Ok(trigger)
    }

    pub fn replaced(&self, mut next: StoredTrigger, epoch: u64) -> Result<StoredTrigger> {
        next.name = self.name.clone();
        next.version = self.version + 1;
        next.created_epoch = self.created_epoch;
        next.updated_epoch = epoch;
        next.checksum = checksum(ChecksumInput {
            name: &next.name,
            version: next.version,
            target: &next.target,
            timing: next.timing,
            event: next.event,
            update_of: &next.update_of,
            target_columns: &next.target_columns,
            when: &next.when,
            program: &next.program,
        })?;
        next.validate()?;
        Ok(next)
    }

    pub fn retarget_table(&self, new_name: impl Into<String>, epoch: u64) -> Result<Self> {
        let mut next = self.clone();
        next.version = next.version.saturating_add(1);
        next.target = TriggerTarget::Table(new_name.into());
        next.updated_epoch = epoch;
        next.refresh_checksum()?;
        Ok(next)
    }

    pub fn renamed_update_column(
        &self,
        old_name: &str,
        new_name: impl Into<String>,
        epoch: u64,
    ) -> Result<Self> {
        let new_name = new_name.into();
        let mut next = self.clone();
        let mut changed = false;
        for column in &mut next.update_of {
            if column == old_name {
                *column = new_name.clone();
                changed = true;
            }
        }
        if changed {
            next.version = next.version.saturating_add(1);
            next.updated_epoch = epoch;
            next.refresh_checksum()?;
        }
        Ok(next)
    }

    pub fn validate(&self) -> Result<()> {
        validate_name("trigger", &self.name)?;
        match &self.target {
            TriggerTarget::Table(name) => validate_name("trigger table target", name)?,
            TriggerTarget::View(name) => validate_name("trigger view target", name)?,
        }
        match (&self.target, self.timing) {
            (TriggerTarget::Table(_), TriggerTiming::Before | TriggerTiming::After) => {
                if !self.target_columns.is_empty() {
                    return Err(MongrelError::InvalidArgument(
                        "table triggers must not carry target_columns".into(),
                    ));
                }
            }
            (TriggerTarget::View(_), TriggerTiming::InsteadOf) => {
                validate_target_columns(&self.target_columns)?;
            }
            (TriggerTarget::Table(_), TriggerTiming::InsteadOf) => {
                return Err(MongrelError::InvalidArgument(
                    "INSTEAD OF triggers target views, not tables".into(),
                ));
            }
            (TriggerTarget::View(_), TriggerTiming::Before | TriggerTiming::After) => {
                return Err(MongrelError::InvalidArgument(
                    "views support INSTEAD OF triggers, not BEFORE/AFTER triggers".into(),
                ));
            }
        }
        let mut update_cols = HashSet::new();
        for col in &self.update_of {
            validate_name("trigger UPDATE OF column", col)?;
            if !update_cols.insert(col) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate trigger UPDATE OF column {col:?}"
                )));
            }
        }
        for step in &self.program.steps {
            step.validate()?;
        }
        Ok(())
    }

    fn refresh_checksum(&mut self) -> Result<()> {
        self.checksum = checksum(ChecksumInput {
            name: &self.name,
            version: self.version,
            target: &self.target,
            timing: self.timing,
            event: self.event,
            update_of: &self.update_of,
            target_columns: &self.target_columns,
            when: &self.when,
            program: &self.program,
        })?;
        self.validate()
    }
}

impl TriggerStep {
    fn validate(&self) -> Result<()> {
        match self {
            TriggerStep::SetNew { cells } => {
                let mut seen = HashSet::new();
                for cell in cells {
                    if !seen.insert(cell.column_id) {
                        return Err(MongrelError::InvalidArgument(format!(
                            "duplicate trigger cell column id {}",
                            cell.column_id
                        )));
                    }
                }
                Ok(())
            }
            TriggerStep::Insert { table, cells } | TriggerStep::UpdateByPk { table, cells, .. } => {
                validate_name("trigger step table", table)?;
                let mut seen = HashSet::new();
                for cell in cells {
                    if !seen.insert(cell.column_id) {
                        return Err(MongrelError::InvalidArgument(format!(
                            "duplicate trigger cell column id {}",
                            cell.column_id
                        )));
                    }
                }
                Ok(())
            }
            TriggerStep::DeleteByPk { table, .. } | TriggerStep::Select { table, .. } => {
                validate_name("trigger step table", table)
            }
            TriggerStep::DeleteWhere { table, .. } => validate_name("trigger step table", table),
            TriggerStep::UpdateWhere { table, cells, .. } => {
                validate_name("trigger step table", table)?;
                let mut seen = HashSet::new();
                for cell in cells {
                    if !seen.insert(cell.column_id) {
                        return Err(MongrelError::InvalidArgument(format!(
                            "duplicate trigger cell column id {}",
                            cell.column_id
                        )));
                    }
                }
                Ok(())
            }
            TriggerStep::Foreach { id, .. } => {
                if id.is_empty() {
                    return Err(MongrelError::InvalidArgument(
                        "foreach id must not be empty".into(),
                    ));
                }
                Ok(())
            }
            TriggerStep::Raise { .. } => Ok(()),
        }
    }
}

impl From<StoredTrigger> for TriggerEntry {
    fn from(trigger: StoredTrigger) -> Self {
        Self { trigger }
    }
}

#[derive(Serialize)]
struct ChecksumInput<'a> {
    name: &'a str,
    version: u64,
    target: &'a TriggerTarget,
    timing: TriggerTiming,
    event: TriggerEvent,
    update_of: &'a [String],
    target_columns: &'a [ColumnDef],
    when: &'a Option<TriggerExpr>,
    program: &'a TriggerProgram,
}

fn checksum(input: ChecksumInput<'_>) -> Result<String> {
    let bytes = serde_json::to_vec(&input)
        .map_err(|e| MongrelError::Other(format!("trigger checksum serialize: {e}")))?;
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

fn validate_target_columns(columns: &[ColumnDef]) -> Result<()> {
    if columns.is_empty() {
        return Err(MongrelError::InvalidArgument(
            "view triggers require target_columns".into(),
        ));
    }
    let mut names = HashSet::new();
    let mut ids = HashSet::new();
    for column in columns {
        validate_name("trigger target column", &column.name)?;
        if !names.insert(&column.name) {
            return Err(MongrelError::InvalidArgument(format!(
                "duplicate trigger target column {:?}",
                column.name
            )));
        }
        if !ids.insert(column.id) {
            return Err(MongrelError::InvalidArgument(format!(
                "duplicate trigger target column id {}",
                column.id
            )));
        }
    }
    Ok(())
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
