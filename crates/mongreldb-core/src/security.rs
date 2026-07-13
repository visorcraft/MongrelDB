//! Persistent row policies and column masking.

use crate::auth::Principal;
use crate::memtable::{Row, Value};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SecurityCatalog {
    #[serde(default)]
    pub rls_tables: Vec<String>,
    #[serde(default)]
    pub policies: Vec<RowPolicy>,
    #[serde(default)]
    pub masks: Vec<ColumnMask>,
}

/// Security state used to evaluate only scored retrieval candidates.
pub struct CandidateAuthorization<'a> {
    pub table: &'a str,
    pub security: &'a SecurityCatalog,
    pub principal: &'a Principal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RowPolicy {
    pub name: String,
    pub table: String,
    pub command: PolicyCommand,
    #[serde(default)]
    pub subjects: Vec<String>,
    #[serde(default = "default_true")]
    pub permissive: bool,
    pub using: Option<SecurityExpr>,
    pub with_check: Option<SecurityExpr>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolicyCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SecurityExpr {
    True,
    ColumnEqCurrentUser {
        column: u16,
    },
    ColumnEqValue {
        column: u16,
        value: Value,
    },
    And {
        left: Box<SecurityExpr>,
        right: Box<SecurityExpr>,
    },
    Or {
        left: Box<SecurityExpr>,
        right: Box<SecurityExpr>,
    },
    Not {
        expression: Box<SecurityExpr>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnMask {
    pub name: String,
    pub table: String,
    pub column: u16,
    pub strategy: MaskStrategy,
    #[serde(default)]
    pub exempt_subjects: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MaskStrategy {
    Null,
    Redact { replacement: String },
    Sha256,
}

impl SecurityCatalog {
    pub fn table_has_security(&self, table: &str) -> bool {
        self.rls_tables.iter().any(|name| name == table)
            || self.masks.iter().any(|mask| mask.table == table)
    }

    pub fn table_has_objects(&self, table: &str) -> bool {
        self.table_has_security(table) || self.policies.iter().any(|policy| policy.table == table)
    }

    pub fn rls_enabled(&self, table: &str) -> bool {
        self.rls_tables.iter().any(|name| name == table)
    }

    pub fn row_allowed(
        &self,
        table: &str,
        command: PolicyCommand,
        row: &Row,
        principal: &Principal,
        check_new: bool,
    ) -> bool {
        if principal.is_admin || !self.rls_enabled(table) {
            return true;
        }
        let applicable = self.policies.iter().filter(|policy| {
            policy.table == table
                && (matches!(policy.command, PolicyCommand::All) || policy.command == command)
        });
        let mut has_permissive = false;
        let mut permissive_allowed = false;
        let mut restrictive_allowed = true;
        for policy in applicable.filter(|policy| subject_matches(&policy.subjects, principal)) {
            let expression = if check_new {
                policy.with_check.as_ref().or(policy.using.as_ref())
            } else {
                policy.using.as_ref()
            };
            let allowed = expression.is_some_and(|expression| expression.eval(row, principal));
            if policy.permissive {
                has_permissive = true;
                permissive_allowed |= allowed;
            } else {
                restrictive_allowed &= allowed;
            }
        }
        has_permissive && permissive_allowed && restrictive_allowed
    }

    pub fn apply_masks(&self, table: &str, row: &mut Row, principal: &Principal) {
        if principal.is_admin {
            return;
        }
        for mask in self.masks.iter().filter(|mask| mask.table == table) {
            if !mask.exempt_subjects.is_empty() && subject_matches(&mask.exempt_subjects, principal)
            {
                continue;
            }
            let Some(value) = row.columns.get_mut(&mask.column) else {
                continue;
            };
            *value = mask.strategy.apply(value);
        }
    }

    pub fn apply_masks_to_cells(
        &self,
        table: &str,
        cells: &mut [(u16, Value)],
        principal: &Principal,
    ) {
        if principal.is_admin {
            return;
        }
        for mask in self.masks.iter().filter(|mask| mask.table == table) {
            if !mask.exempt_subjects.is_empty() && subject_matches(&mask.exempt_subjects, principal)
            {
                continue;
            }
            if let Some((_, value)) = cells
                .iter_mut()
                .find(|(column_id, _)| *column_id == mask.column)
            {
                *value = mask.strategy.apply(value);
            }
        }
    }

    /// Columns needed to evaluate applicable SELECT policies for one principal.
    pub fn select_policy_columns(&self, table: &str, principal: &Principal) -> Vec<u16> {
        let mut columns = Vec::new();
        for policy in self.policies.iter().filter(|policy| {
            policy.table == table
                && (matches!(policy.command, PolicyCommand::All)
                    || policy.command == PolicyCommand::Select)
                && subject_matches(&policy.subjects, principal)
        }) {
            if let Some(expression) = &policy.using {
                expression.collect_columns(&mut columns);
            }
        }
        columns.sort_unstable();
        columns.dedup();
        columns
    }
}

impl SecurityExpr {
    pub fn eval(&self, row: &Row, principal: &Principal) -> bool {
        match self {
            SecurityExpr::True => true,
            SecurityExpr::ColumnEqCurrentUser { column } => {
                row.columns.get(column).is_some_and(|value| {
                    value == &Value::Bytes(principal.username.as_bytes().to_vec())
                })
            }
            SecurityExpr::ColumnEqValue { column, value } => row
                .columns
                .get(column)
                .is_some_and(|current| current == value),
            SecurityExpr::And { left, right } => {
                left.eval(row, principal) && right.eval(row, principal)
            }
            SecurityExpr::Or { left, right } => {
                left.eval(row, principal) || right.eval(row, principal)
            }
            SecurityExpr::Not { expression } => !expression.eval(row, principal),
        }
    }

    fn collect_columns(&self, columns: &mut Vec<u16>) {
        match self {
            SecurityExpr::True => {}
            SecurityExpr::ColumnEqCurrentUser { column }
            | SecurityExpr::ColumnEqValue { column, .. } => columns.push(*column),
            SecurityExpr::And { left, right } | SecurityExpr::Or { left, right } => {
                left.collect_columns(columns);
                right.collect_columns(columns);
            }
            SecurityExpr::Not { expression } => expression.collect_columns(columns),
        }
    }
}

impl MaskStrategy {
    fn apply(&self, value: &Value) -> Value {
        match self {
            MaskStrategy::Null => Value::Null,
            MaskStrategy::Redact { replacement } => match value {
                Value::Null => Value::Null,
                Value::Bytes(_) => Value::Bytes(replacement.as_bytes().to_vec()),
                _ => Value::Null,
            },
            MaskStrategy::Sha256 => match value {
                Value::Null => Value::Null,
                Value::Bytes(bytes) => Value::Bytes(
                    Sha256::digest(bytes)
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>()
                        .into_bytes(),
                ),
                _ => Value::Null,
            },
        }
    }
}

fn subject_matches(subjects: &[String], principal: &Principal) -> bool {
    subjects.is_empty()
        || subjects.iter().any(|subject| {
            subject.eq_ignore_ascii_case("public")
                || subject == &principal.username
                || principal.roles.contains(subject)
        })
}

fn default_true() -> bool {
    true
}
