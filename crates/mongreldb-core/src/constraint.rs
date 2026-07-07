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
///
/// The [`CheckExpr::Regex`] variant stores the pattern string for serde/equality
/// and caches the compiled [`regex::Regex`] in a [`std::sync::OnceLock`] (populated
/// on first evaluation). The cache is `#[serde(skip)]` and excluded from
/// [`PartialEq`].
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Regex pattern match against a column's value (PostgreSQL `~`/`~*`/`!~`/`!~*`).
    /// The column value must be `Value::Bytes` (UTF-8 string); non-bytes and null
    /// yield `Unknown` (CHECK passes). `negated` inverts the match result;
    /// `case_insensitive` enables case-insensitive matching. The Rust `regex`
    /// crate is linear-time (no catastrophic backtracking), so ReDoS is not a
    /// concern.
    Regex {
        col: u16,
        pattern: String,
        negated: bool,
        case_insensitive: bool,
        #[serde(skip)]
        cached: std::sync::OnceLock<regex::Regex>,
    },
}

impl PartialEq for CheckExpr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::True, Self::True) => true,
            (Self::Col(a), Self::Col(b)) => a == b,
            (Self::Lit(a), Self::Lit(b)) => a == b,
            (Self::IsNull(a), Self::IsNull(b)) => a == b,
            (Self::IsNotNull(a), Self::IsNotNull(b)) => a == b,
            (Self::Eq(a1, a2), Self::Eq(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Ne(a1, a2), Self::Ne(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Lt(a1, a2), Self::Lt(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Le(a1, a2), Self::Le(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Gt(a1, a2), Self::Gt(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Ge(a1, a2), Self::Ge(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::And(a1, a2), Self::And(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Or(a1, a2), Self::Or(b1, b2)) => a1 == b1 && a2 == b2,
            (Self::Not(a), Self::Not(b)) => a == b,
            (
                Self::Regex {
                    col: ac,
                    pattern: ap,
                    negated: an,
                    case_insensitive: ai,
                    ..
                },
                Self::Regex {
                    col: bc,
                    pattern: bp,
                    negated: bn,
                    case_insensitive: bi,
                    ..
                },
            ) => ac == bc && ap == bp && an == bn && ai == bi,
            _ => false,
        }
    }
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

    /// Validate that all regex patterns in this expression compile successfully.
    /// Call at DDL time (table creation / constraint addition) so invalid
    /// patterns are rejected eagerly rather than silently never-matching at
    /// first commit.
    pub fn validate(&self) -> Result<()> {
        match self {
            CheckExpr::Eq(a, b)
            | CheckExpr::Ne(a, b)
            | CheckExpr::Lt(a, b)
            | CheckExpr::Le(a, b)
            | CheckExpr::Gt(a, b)
            | CheckExpr::Ge(a, b)
            | CheckExpr::And(a, b)
            | CheckExpr::Or(a, b) => {
                a.validate()?;
                b.validate()?;
            }
            CheckExpr::Not(a) => a.validate()?,
            CheckExpr::Regex { pattern, .. } => {
                regex::Regex::new(pattern).map_err(|e| {
                    MongrelError::InvalidArgument(format!("invalid regex pattern: {e}"))
                })?;
            }
            _ => {}
        }
        Ok(())
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
            CheckExpr::Regex {
                col,
                pattern,
                negated,
                case_insensitive,
                cached,
            } => match cells.get(col) {
                None | Some(Value::Null) => Tri::Unknown,
                Some(Value::Bytes(b)) => {
                    let re = cached.get_or_init(|| {
                        let mut builder = regex::RegexBuilder::new(pattern);
                        builder.case_insensitive(*case_insensitive);
                        // Invalid patterns are rejected at DDL validation; if we
                        // reach here with an invalid pattern, create a regex that
                        // never matches (safe default).
                        builder
                            .build()
                            .unwrap_or_else(|_| regex::Regex::new("$^").unwrap())
                    });
                    let matched = re.is_match(std::str::from_utf8(b).unwrap_or(""));
                    match (*negated, matched) {
                        (false, true) | (true, false) => Tri::True,
                        (false, false) | (true, true) => Tri::False,
                    }
                }
                // Non-bytes values (numbers, bools, etc.) are not regex-matchable.
                Some(_) => Tri::Unknown,
            },
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
            Value::Uuid(_) | Value::Json(_) => Tri::Unknown,
            Value::Decimal(d) => {
                if *d != 0 {
                    Tri::True
                } else {
                    Tri::False
                }
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

    fn regex_expr(col: u16, pattern: &str, negated: bool, ci: bool) -> CheckExpr {
        CheckExpr::Regex {
            col,
            pattern: pattern.to_string(),
            negated,
            case_insensitive: ci,
            cached: std::sync::OnceLock::new(),
        }
    }

    #[test]
    fn check_regex_match() {
        let e = regex_expr(1, r"^a\w+z$", false, false);
        assert!(e.satisfied(&m(&[(1, Value::Bytes(b"abz".to_vec()))])));
        assert!(e.satisfied(&m(&[(1, Value::Bytes(b"alfredz".to_vec()))])));
        assert!(!e.satisfied(&m(&[(1, Value::Bytes(b"abc".to_vec()))])));
    }

    #[test]
    fn check_regex_null_is_unknown() {
        let e = regex_expr(1, r"^\d+$", false, false);
        assert!(e.satisfied(&m(&[(1, Value::Null)])));
        assert!(e.satisfied(&m(&[]))); // column absent
    }

    #[test]
    fn check_regex_negated() {
        let e = regex_expr(1, r"^\d+$", true, false);
        assert!(e.satisfied(&m(&[(1, Value::Bytes(b"abc".to_vec()))])));
        assert!(!e.satisfied(&m(&[(1, Value::Bytes(b"123".to_vec()))])));
    }

    #[test]
    fn check_regex_case_insensitive() {
        let e = regex_expr(1, r"^hello$", false, true);
        assert!(e.satisfied(&m(&[(1, Value::Bytes(b"HELLO".to_vec()))])));
        assert!(e.satisfied(&m(&[(1, Value::Bytes(b"Hello".to_vec()))])));
    }

    #[test]
    fn check_regex_non_bytes_is_unknown() {
        let e = regex_expr(1, r"\d+", false, false);
        // Int64 is not regex-matchable → Unknown → satisfied
        assert!(e.satisfied(&m(&[(1, Value::Int64(42))])));
    }

    #[test]
    fn check_regex_validate_rejects_invalid_pattern() {
        assert!(regex_expr(1, "[", false, false).validate().is_err());
        assert!(regex_expr(1, r"^\d+$", false, false).validate().is_ok());
    }

    #[test]
    fn check_regex_partial_eq_ignores_cache() {
        let a = regex_expr(1, r"^\d+$", false, false);
        let b = regex_expr(1, r"^\d+$", false, false);
        assert_eq!(a, b);
        // Populate cache on one — equality still holds because cached is ignored.
        a.satisfied(&m(&[(1, Value::Bytes(b"1".to_vec()))]));
        assert_eq!(a, b);
    }

    #[test]
    fn check_regex_serde_roundtrip() {
        let e = regex_expr(1, r"^\w+@[\w.]+$", true, true);
        let json = serde_json::to_string(&e).unwrap();
        // Ensure the `cached` OnceLock field is NOT serialized.
        assert!(!json.contains("cached"));
        let de: CheckExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(e, de);
        // Deserialized regex still evaluates correctly.
        assert!(de.satisfied(&m(&[(1, Value::Bytes(b"not-an-email".to_vec()))])));
        assert!(!de.satisfied(&m(&[(1, Value::Bytes(b"a@b.com".to_vec()))])));
    }

    #[test]
    fn check_regex_inside_logical_ops() {
        // col 1 matches digits AND col 2 is not null
        let e = CheckExpr::And(
            Box::new(regex_expr(1, r"^\d+$", false, false)),
            Box::new(CheckExpr::IsNotNull(2)),
        );
        assert!(e.satisfied(&m(&[
            (1, Value::Bytes(b"42".to_vec())),
            (2, Value::Int64(1))
        ])));
        assert!(!e.satisfied(&m(&[(1, Value::Bytes(b"42".to_vec())), (2, Value::Null)])));
        assert!(!e.satisfied(&m(&[
            (1, Value::Bytes(b"abc".to_vec())),
            (2, Value::Int64(1))
        ])));
    }
}
