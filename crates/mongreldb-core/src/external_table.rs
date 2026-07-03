use crate::error::{MongrelError, Result};
use crate::schema::Schema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExternalTableEntry {
    pub name: String,
    pub module: String,
    #[serde(default)]
    pub args: Vec<ModuleArg>,
    pub declared_schema: Schema,
    #[serde(default)]
    pub hidden_columns: Vec<String>,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
    #[serde(default)]
    pub capabilities: ModuleCapabilities,
    pub created_epoch: u64,
}

#[derive(Debug, Clone)]
pub struct ExternalTableDefinition {
    pub module: String,
    pub args: Vec<ModuleArg>,
    pub declared_schema: Schema,
    pub hidden_columns: Vec<String>,
    pub options: BTreeMap<String, String>,
    pub capabilities: ModuleCapabilities,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum ModuleArg {
    Ident(String),
    String(String),
    Number(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModuleCapabilities {
    pub read_only: bool,
    pub insert_only: bool,
    pub writable: bool,
    pub deterministic: bool,
    pub trigger_safe: bool,
    pub transaction_safe: bool,
}

impl ExternalTableEntry {
    pub fn new(
        name: impl Into<String>,
        definition: ExternalTableDefinition,
        created_epoch: u64,
    ) -> Result<Self> {
        let entry = Self {
            name: name.into(),
            module: definition.module,
            args: definition.args,
            declared_schema: definition.declared_schema,
            hidden_columns: definition.hidden_columns,
            options: definition.options,
            capabilities: definition.capabilities,
            created_epoch,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn validate(&self) -> Result<()> {
        validate_name("external table", &self.name)?;
        validate_name("external table module", &self.module)?;
        let mut hidden = HashSet::new();
        for col in &self.hidden_columns {
            validate_name("external table hidden column", col)?;
            if !hidden.insert(col) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate external table hidden column {col:?}"
                )));
            }
        }
        self.declared_schema.validate_auto_increment()?;
        Ok(())
    }
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
