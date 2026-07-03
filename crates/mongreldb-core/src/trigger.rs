use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use crate::schema::ColumnDef;
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
}

impl Default for TriggerConfig {
    fn default() -> Self {
        Self {
            recursive_triggers: false,
            max_depth: 32,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerProgram {
    #[serde(default)]
    pub steps: Vec<TriggerStep>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
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
        #[serde(default)]
        conditions: Vec<TriggerCondition>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TriggerCondition {
    Pk { value: TriggerValue },
    Eq { column_id: u16, value: TriggerValue },
    IsNull { column_id: u16 },
    IsNotNull { column_id: u16 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum TriggerValue {
    Literal(Value),
    NewColumn(u16),
    OldColumn(u16),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
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
    IsNull(TriggerValue),
    IsNotNull(TriggerValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerRaiseAction {
    Abort,
    Fail,
    Rollback,
    Ignore,
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
