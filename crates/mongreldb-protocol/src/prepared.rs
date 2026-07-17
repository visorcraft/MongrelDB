//! Prepared-statement binding record (spec section 10.4, S1D-005).
//!
//! A prepared statement's plan is only valid against exactly the catalog and
//! schema state it was planned against. [`PreparedStatementBinding`] records
//! everything the plan binds to — SQL text, parameter types, catalog
//! version, per-table schema versions, and the negotiated feature set — and
//! [`PreparedStatementBinding::is_compatible`] is the invalidation check the
//! executor runs before executing: on any incompatible schema change the
//! statement is invalidated and replanned. A stale plan MUST never execute
//! silently (S1D-005); executors report incompatibility as
//! [`mongreldb_types::errors::ErrorCategory::SchemaVersionMismatch`], which
//! routes the client through re-preparation (spec section 11.7).

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};

use mongreldb_types::ids::{MetadataVersion, SchemaVersion, TableId};

/// Session-scoped handle of a prepared statement, allocated by
/// [`crate::services::QueryService::prepare`]. Valid only within the session
/// that prepared it; the zero value is reserved.
#[repr(transparent)]
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct StatementId(pub u64);

impl StatementId {
    /// The zero value (reserved).
    pub const ZERO: Self = Self(0);

    /// Wraps a raw value.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for StatementId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for StatementId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StatementId({})", self.0)
    }
}

/// Everything a prepared plan binds to (S1D-005).
///
/// The binding is data only: the plan itself is server state. Executors call
/// [`Self::is_compatible`] before every execution and invalidate + replan on
/// `false`; a stale plan never executes silently.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PreparedStatementBinding {
    /// Session-scoped handle of this prepared statement.
    pub statement_id: StatementId,
    /// The SQL text the plan was built from.
    pub sql: String,
    /// Canonical parameter type names in statement order (see
    /// [`crate::request::ParameterValue::type_name`]); execution with a
    /// mismatched parameter list fails rather than coercing silently.
    pub parameter_types: Vec<String>,
    /// Catalog metadata version the plan was built against.
    pub catalog_version: MetadataVersion,
    /// Schema version of every table the plan touches, keyed by table.
    pub schema_versions: BTreeMap<TableId, SchemaVersion>,
    /// The negotiated engine feature set the plan relies on. Feature-set
    /// changes are negotiated at session (re)handshake, not per request, so
    /// [`Self::is_compatible`] does not re-check them; a session whose
    /// feature set changed re-prepares all of its statements.
    pub feature_set: BTreeSet<String>,
}

impl PreparedStatementBinding {
    /// Whether the plan is still valid against the current catalog state.
    ///
    /// Compatible iff the catalog version is unchanged AND every table the
    /// plan touches still exists at exactly the schema version it was
    /// planned against. Tables the plan does not touch may change freely.
    ///
    /// On `false` the statement MUST be invalidated and replanned (S1D-005):
    /// a stale plan never executes silently.
    pub fn is_compatible(
        &self,
        catalog_version: MetadataVersion,
        schema_versions: &BTreeMap<TableId, SchemaVersion>,
    ) -> bool {
        self.catalog_version == catalog_version
            && self
                .schema_versions
                .iter()
                .all(|(table, version)| schema_versions.get(table) == Some(version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::assert_serde_round_trip;

    fn binding() -> PreparedStatementBinding {
        let mut schema_versions = BTreeMap::new();
        schema_versions.insert(TableId::new(1), SchemaVersion::new(10));
        schema_versions.insert(TableId::new(2), SchemaVersion::new(20));
        let mut feature_set = BTreeSet::new();
        feature_set.insert("ann-index".to_owned());
        feature_set.insert("cdc".to_owned());
        PreparedStatementBinding {
            statement_id: StatementId::new(7),
            sql: "SELECT * FROM events WHERE tenant = ?".to_owned(),
            parameter_types: vec!["INT64".to_owned()],
            catalog_version: MetadataVersion::new(100),
            schema_versions,
            feature_set,
        }
    }

    #[test]
    fn statement_id_basics_and_serde() {
        assert_eq!(StatementId::ZERO.get(), 0);
        let id = StatementId::new(42);
        assert_eq!(id.get(), 42);
        assert_eq!(id.to_string(), "42");
        assert_eq!(format!("{id:?}"), "StatementId(42)");
        assert_serde_round_trip(&id);
        assert_serde_round_trip(&StatementId::ZERO);
    }

    #[test]
    fn binding_serde_round_trip() {
        assert_serde_round_trip(&binding());
        let empty = PreparedStatementBinding {
            statement_id: StatementId::ZERO,
            sql: String::new(),
            parameter_types: vec![],
            catalog_version: MetadataVersion::ZERO,
            schema_versions: BTreeMap::new(),
            feature_set: BTreeSet::new(),
        };
        assert_serde_round_trip(&empty);
    }

    #[test]
    fn invalidation_matrix() {
        let binding = binding();
        let current = binding.schema_versions.clone();

        // Unchanged state is compatible.
        assert!(binding.is_compatible(MetadataVersion::new(100), &current));

        // Catalog version bump invalidates.
        assert!(!binding.is_compatible(MetadataVersion::new(101), &current));

        // Schema change on a touched table invalidates.
        let mut altered = current.clone();
        altered.insert(TableId::new(1), SchemaVersion::new(11));
        assert!(!binding.is_compatible(MetadataVersion::new(100), &altered));

        // Dropping a touched table invalidates.
        let mut dropped = current.clone();
        dropped.remove(&TableId::new(2));
        assert!(!binding.is_compatible(MetadataVersion::new(100), &dropped));

        // Changing a table the plan does not touch stays compatible.
        let mut unrelated = current.clone();
        unrelated.insert(TableId::new(3), SchemaVersion::new(1));
        assert!(binding.is_compatible(MetadataVersion::new(100), &unrelated));

        // A binding over no tables only depends on the catalog version.
        let no_tables = PreparedStatementBinding {
            schema_versions: BTreeMap::new(),
            ..binding.clone()
        };
        assert!(no_tables.is_compatible(MetadataVersion::new(100), &BTreeMap::new()));
        assert!(!no_tables.is_compatible(MetadataVersion::new(99), &BTreeMap::new()));
    }
}
