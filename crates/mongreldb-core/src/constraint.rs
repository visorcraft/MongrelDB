//! Engine-side declarative constraints (unique, foreign key, check) enforced
//! authoritatively inside the core transaction path. Opt-in per-table via
//! [`crate::schema::Schema::constraints`]; tables with an empty constraint set
//! (the default — including every legacy table and every Kit-managed table)
//! behave exactly as before. This subsystem is independent of the Kit's own
//! guard-table mechanism: the Kit continues to enforce its constraints exactly
//! as before, and these engine constraints only fire for tables whose schema
//! carries a non-empty [`TableConstraints`].
//!
//! Enforcement is performed in [`crate::database::Database::commit_transaction`]
//! as a pre-sequencer validation pass (under the transaction's read snapshot,
//! outside the WAL mutex) plus `WriteKey::Unique` registration so that two
//! concurrent transactions inserting the same key cannot both commit.

use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;

/// A declarative constraint set attached to a table's schema. Empty by default.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TableConstraints {
    #[serde(default)]
    pub uniques: Vec<UniqueConstraint>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(default)]
    pub checks: Vec<CheckConstraint>,
}

impl TableConstraints {
    pub fn is_empty(&self) -> bool {
        self.uniques.is_empty() && self.foreign_keys.is_empty() && self.checks.is_empty()
    }
}

/// A multi-column uniqueness constraint (beyond the single-column `PRIMARY_KEY`
/// flag). Enforced via an existence scan against the read snapshot plus
/// `WriteKey::Unique` registration at commit (first-committer-wins).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct UniqueConstraint {
    pub id: u16,
    pub name: String,
    pub columns: Vec<u16>,
}

/// ON DELETE action for a [`ForeignKey`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum FkAction {
    /// Reject the parent delete if any child row references it (default).
    #[default]
    Restrict,
    /// Cascade the delete to child rows.
    Cascade,
    /// Set the referencing columns to `NULL` in child rows.
    SetNull,
}

/// A foreign key: the listed `columns` must reference an existing row in
/// `ref_table` whose `ref_columns` match. The parent table is named by string so
/// the link survives the parent's numeric table-id assignment across reopens.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKey {
    pub id: u16,
    pub name: String,
    pub columns: Vec<u16>,
    pub ref_table: String,
    pub ref_columns: Vec<u16>,
    #[serde(default)]
    pub on_delete: FkAction,
}

/// A CHECK constraint: the row is rejected when `expr` evaluates to `False`
/// (SQL three-valued logic: `Unknown`/`True` both pass).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckConstraint {
    pub id: u16,
    pub name: String,
    pub expr: CheckExpr,
}

/// A minimal boolean expression IR for CHECK constraints, evaluated against a
/// row's cells. Terms ([`CheckExpr::Col`] / [`CheckExpr::Lit`]) resolve to
/// [`Value`]; comparisons and logical ops are boolean. Any comparison involving
/// `Value::Null` yields three-valued `Unknown`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CheckExpr {
    /// Constant true (the trivially-satisfied CHECK).
    True,
    /// A bare column reference used as a boolean (truthiness coercion).
    Col(u16),
    /// A literal value.
    Lit(Value),
    IsNull(u16),
    IsNotNull(u16),
    Eq(Box<CheckExpr>, Box<CheckExpr>),
    Ne(Box<CheckExpr>, Box<CheckExpr>),
    Lt(Box<CheckExpr>, Box<CheckExpr>),
    Le(Box<CheckExpr>, Box<CheckExpr>),
    Gt(Box<CheckExpr>, Box<CheckExpr>),
    Ge(Box<CheckExpr>, Box<CheckExpr>),
    And(Box<CheckExpr>, Box<CheckExpr>),
    Or(Box<CheckExpr>, Box<CheckExpr>),
    Not(Box<CheckExpr>),
}

/// Three-valued logic result for CHECK evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tri {
    True,
    False,
    Unknown,
}

impl CheckExpr {
    /// Evaluate the expression against a row's cells. A CHECK constraint is
    /// satisfied unless this returns [`Tri::False`].
    pub fn satisfied(&self, cells: &HashMap<u16, Value>) -> bool {
        !matches!(self.eval(cells), Tri::False)
    }

    fn eval(&self, cells: &HashMap<u16, Value>) -> Tri {
        match self {
            CheckExpr::True => Tri::True,
            CheckExpr::Col(id) => match cells.get(id) {
                None | Some(Value::Null) => Tri::Unknown,
                Some(v) => Tri::from_truthy(v),
            },
            CheckExpr::Lit(v) => Tri::from_truthy(v),
            CheckExpr::IsNull(id) => match cells.get(id) {
                None | Some(Value::Null) => Tri::True,
                Some(_) => Tri::False,
            },
            CheckExpr::IsNotNull(id) => match cells.get(id) {
                None | Some(Value::Null) => Tri::False,
                Some(_) => Tri::True,
            },
            CheckExpr::Eq(a, b) => compare(a.eval_term(cells), b.eval_term(cells), |o| {
                o == Ordering::Equal
            }),
            CheckExpr::Ne(a, b) => compare(a.eval_term(cells), b.eval_term(cells), |o| {
                o != Ordering::Equal
            }),
            CheckExpr::Lt(a, b) => compare(a.eval_term(cells), b.eval_term(cells), |o| {
                o == Ordering::Less
            }),
            CheckExpr::Le(a, b) => compare(a.eval_term(cells), b.eval_term(cells), |o| {
                o != Ordering::Greater
            }),
            CheckExpr::Gt(a, b) => compare(a.eval_term(cells), b.eval_term(cells), |o| {
                o == Ordering::Greater
            }),
            CheckExpr::Ge(a, b) => compare(a.eval_term(cells), b.eval_term(cells), |o| {
                o != Ordering::Less
            }),
            CheckExpr::And(a, b) => and3(a.eval(cells), b.eval(cells)),
            CheckExpr::Or(a, b) => or3(a.eval(cells), b.eval(cells)),
            CheckExpr::Not(a) => not3(a.eval(cells)),
        }
    }

    /// Evaluate a term (Col/Lit) to a [`Value`]; boolean expressions coerce via
    /// truthiness (True→1, False/Unknown→Null) so they can still be compared.
    fn eval_term(&self, cells: &HashMap<u16, Value>) -> Value {
        match self {
            CheckExpr::Col(id) => cells.get(id).cloned().unwrap_or(Value::Null),
            CheckExpr::Lit(v) => v.clone(),
            other => match other.eval(cells) {
                Tri::True => Value::Int64(1),
                Tri::False | Tri::Unknown => Value::Null,
            },
        }
    }
}

impl Tri {
    fn from_truthy(v: &Value) -> Tri {
        match v {
            Value::Null => Tri::Unknown,
            Value::Bool(b) => {
                if *b {
                    Tri::True
                } else {
                    Tri::False
                }
            }
            Value::Int64(n) => {
                if *n != 0 {
                    Tri::True
                } else {
                    Tri::False
                }
            }
            Value::Float64(f) => {
                if *f != 0.0 {
                    Tri::True
                } else {
                    Tri::False
                }
            }
            Value::Bytes(b) => {
                if b.is_empty() {
                    Tri::False
                } else {
                    Tri::True
                }
            }
            Value::Embedding(v) => {
                if v.is_empty() {
                    Tri::False
                } else {
                    Tri::True
                }
            }
            Value::Interval { .. } => Tri::Unknown,
            Value::Decimal(d) => {
                if *d != 0 { Tri::True } else { Tri::False }
            }
        }
    }
}

fn and3(a: Tri, b: Tri) -> Tri {
    match (a, b) {
        (Tri::False, _) | (_, Tri::False) => Tri::False,
        (Tri::Unknown, _) | (_, Tri::Unknown) => Tri::Unknown,
        _ => Tri::True,
    }
}

fn or3(a: Tri, b: Tri) -> Tri {
    match (a, b) {
        (Tri::True, _) | (_, Tri::True) => Tri::True,
        (Tri::Unknown, _) | (_, Tri::Unknown) => Tri::Unknown,
        _ => Tri::False,
    }
}

fn not3(a: Tri) -> Tri {
    match a {
        Tri::True => Tri::False,
        Tri::False => Tri::True,
        Tri::Unknown => Tri::Unknown,
    }
}

/// Run a comparison from two values. If either term is `Null` the comparison is
/// `Unknown`; otherwise apply `pred` to the concrete ordering (incomparable
/// types also yield `Unknown`).
fn compare(a: Value, b: Value, pred: impl Fn(Ordering) -> bool) -> Tri {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Tri::Unknown;
    }
    match value_cmp(&a, &b) {
        Some(o) => {
            if pred(o) {
                Tri::True
            } else {
                Tri::False
            }
        }
        None => Tri::Unknown,
    }
}

/// Cross-type comparison for values. Same-shape values compare naturally;
/// Int64/Float64 cross-compare numerically; Bytes lexicographically; Bool as
/// 0/1; other distinct-type pairs are `None` (incomparable).
pub(crate) fn value_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        (Value::Bool(x), Value::Bool(y)) => Some((*x as u8).cmp(&(*y as u8))),
        (Value::Int64(x), Value::Int64(y)) => Some(x.cmp(y)),
        (Value::Float64(x), Value::Float64(y)) => x.partial_cmp(y),
        (Value::Int64(x), Value::Float64(y)) => (*x as f64).partial_cmp(y),
        (Value::Float64(x), Value::Int64(y)) => x.partial_cmp(&(*y as f64)),
        (Value::Bytes(x), Value::Bytes(y)) => Some(x.cmp(y)),
        (Value::Embedding(x), Value::Embedding(y)) => {
            for (a, b) in x.iter().zip(y.iter()) {
                match a.partial_cmp(b)? {
                    Ordering::Equal => continue,
                    non_eq => return Some(non_eq),
                }
            }
            Some(x.len().cmp(&y.len()))
        }
        _ => None,
    }
}

/// Encode a set of column values into a stable, unambiguous composite key for
/// uniqueness / FK matching. Returns `None` if any referenced column is missing
/// or `Null` — SQL semantics: a UNIQUE constraint ignores rows where any
/// constrained column is NULL, and an FK with a NULL component is not checked.
pub(crate) fn encode_composite_key(
    columns: &[u16],
    cells: &HashMap<u16, Value>,
) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    for cid in columns {
        let v = cells.get(cid)?;
        if matches!(v, Value::Null) {
            return None;
        }
        let k = v.encode_key();
        // Length-prefix so concatenated multi-column keys stay unambiguous.
        out.extend_from_slice(&(k.len() as u32).to_be_bytes());
        out.extend_from_slice(&k);
    }
    Some(out)
}

/// Validate CHECK constraints for a single staged row.
pub(crate) fn validate_checks(
    checks: &[CheckConstraint],
    cells: &HashMap<u16, Value>,
) -> Result<()> {
    for c in checks {
        if !c.expr.satisfied(cells) {
            return Err(MongrelError::InvalidArgument(format!(
                "CHECK constraint '{}' failed",
                c.name
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(cols: &[(u16, Value)]) -> HashMap<u16, Value> {
        cols.iter().cloned().collect()
    }

    #[test]
    fn check_eq_literal() {
        // col 1 == 5
        let e = CheckExpr::Eq(
            Box::new(CheckExpr::Col(1)),
            Box::new(CheckExpr::Lit(Value::Int64(5))),
        );
        assert!(e.satisfied(&m(&[(1, Value::Int64(5))])));
        assert!(!e.satisfied(&m(&[(1, Value::Int64(6))])));
        // null comparison → unknown → satisfied (CHECK passes on unknown)
        assert!(e.satisfied(&m(&[(1, Value::Null)])));
    }

    #[test]
    fn check_range_and() {
        // 0 <= col1 <= 100
        let e = CheckExpr::And(
            Box::new(CheckExpr::Ge(
                Box::new(CheckExpr::Col(1)),
                Box::new(CheckExpr::Lit(Value::Int64(0))),
            )),
            Box::new(CheckExpr::Le(
                Box::new(CheckExpr::Col(1)),
                Box::new(CheckExpr::Lit(Value::Int64(100))),
            )),
        );
        assert!(e.satisfied(&m(&[(1, Value::Int64(50))])));
        assert!(!e.satisfied(&m(&[(1, Value::Int64(101))])));
        assert!(!e.satisfied(&m(&[(1, Value::Int64(-1))])));
    }

    #[test]
    fn check_numeric_cross_type() {
        let e = CheckExpr::Lt(
            Box::new(CheckExpr::Col(1)),
            Box::new(CheckExpr::Lit(Value::Float64(10.0))),
        );
        assert!(e.satisfied(&m(&[(1, Value::Int64(5))])));
        assert!(!e.satisfied(&m(&[(1, Value::Int64(20))])));
    }

    #[test]
    fn check_not_incomparable_is_unknown_passes() {
        // comparing int to bytes is incomparable → unknown → satisfied
        let e = CheckExpr::Eq(
            Box::new(CheckExpr::Col(1)),
            Box::new(CheckExpr::Lit(Value::Bytes(b"x".to_vec()))),
        );
        assert!(e.satisfied(&m(&[(1, Value::Int64(5))])));
    }

    #[test]
    fn encode_composite_key_skips_null() {
        let k = encode_composite_key(&[1, 2], &m(&[(1, Value::Int64(5)), (2, Value::Null)]));
        assert!(k.is_none());
        let k = encode_composite_key(&[1, 2], &m(&[(1, Value::Int64(5)), (2, Value::Int64(7))]));
        assert!(k.is_some());
        // distinct values → distinct keys
        let k2 = encode_composite_key(&[1, 2], &m(&[(1, Value::Int64(6)), (2, Value::Int64(7))]));
        assert_ne!(k.unwrap(), k2.unwrap());
    }
}
