//! Server-side prepared-statement bindings (spec section 10.4, S1D-005).
//!
//! A prepared plan is only valid against exactly the catalog and schema state
//! it was planned against. This module captures that state from the core
//! catalog into the protocol crate's [`PreparedStatementBinding`] and
//! provides the invalidation check the execute path runs before every
//! execution: on any incompatible catalog/schema change the statement is
//! invalidated and replanned — a stale plan never executes silently.
//!
//! # Version mapping
//!
//! - `catalog_version` maps to the catalog's `db_epoch`: every catalog
//!   mutation (table create/drop/alter/rename, index, trigger, procedure,
//!   view, constraint changes) installs a new catalog at a new epoch, so an
//!   epoch change conservatively invalidates every prepared statement.
//! - `schema_versions` maps each table to a structural FNV-1a hash of its
//!   catalog identity (table id, name, creation epoch, serialized schema), so
//!   a change to one table is observable independently of the others. A
//!   dropped-then-recreated table gets a fresh table id, which the binding
//!   check treats as a schema change.

use std::collections::{BTreeMap, BTreeSet};

use mongreldb_core::Database;
use mongreldb_protocol::prepared::{PreparedStatementBinding, StatementId};
use mongreldb_types::ids::{MetadataVersion, SchemaVersion, TableId};

/// The catalog state a prepared plan binds to (S1D-005).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogState {
    /// Catalog metadata version (`db_epoch`).
    pub(crate) catalog_version: MetadataVersion,
    /// Structural schema version of every live table, keyed by table id.
    pub(crate) schema_versions: BTreeMap<TableId, SchemaVersion>,
}

impl CatalogState {
    /// Snapshot the current catalog state of `db`. Only live tables count:
    /// dropped entries linger in the catalog until their physical state is
    /// reaped, and a plan must never stay valid against a dropped table.
    pub(crate) fn capture(db: &Database) -> Self {
        let catalog = db.catalog_snapshot();
        let schema_versions = catalog
            .tables
            .iter()
            .filter(|entry| matches!(entry.state, mongreldb_core::catalog::TableState::Live))
            .map(|entry| {
                (
                    TableId::new(entry.table_id),
                    schema_version_of(
                        entry.table_id,
                        &entry.name,
                        entry.created_epoch,
                        &entry.schema,
                    ),
                )
            })
            .collect();
        Self {
            catalog_version: MetadataVersion::new(catalog.db_epoch),
            schema_versions,
        }
    }

    /// Whether `binding` is still valid against this state; see
    /// [`PreparedStatementBinding::is_compatible`]. On `false` the caller
    /// MUST invalidate and replan — a stale plan never executes silently.
    pub(crate) fn is_compatible(&self, binding: &PreparedStatementBinding) -> bool {
        binding.is_compatible(self.catalog_version, &self.schema_versions)
    }
}

/// Build the binding recorded when a statement is prepared (S1D-005). The
/// negotiated feature set is empty: this single binary plans with its own
/// feature set, and cross-version feature negotiation lands with the native
/// RPC transport (S1D-002).
pub(crate) fn build_binding(
    statement_id: StatementId,
    sql: String,
    parameter_types: Vec<String>,
    state: &CatalogState,
) -> PreparedStatementBinding {
    PreparedStatementBinding {
        statement_id,
        sql,
        parameter_types,
        catalog_version: state.catalog_version,
        schema_versions: state.schema_versions.clone(),
        feature_set: BTreeSet::new(),
    }
}

/// Structural schema version of one table: FNV-1a over the table's durable
/// catalog identity. Changes exactly when the table's id, name, creation
/// epoch, or schema definition changes.
fn schema_version_of(
    table_id: u64,
    name: &str,
    created_epoch: u64,
    schema: &mongreldb_core::schema::Schema,
) -> SchemaVersion {
    // FNV-1a 64 (the same hash WITHOUT ROWID primary keys use for `RowId`
    // derivation): offset basis then multiply-xor per byte.
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    let mut absorb = |bytes: &[u8]| {
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(PRIME);
        }
    };
    absorb(&table_id.to_le_bytes());
    absorb(name.as_bytes());
    absorb(&created_epoch.to_le_bytes());
    // `Schema` is `Serialize`; a bincode failure is unreachable for this
    // in-memory value, but absorb nothing rather than panicking.
    if let Ok(encoded) = bincode::serialize(schema) {
        absorb(&encoded);
    }
    SchemaVersion::new(hash)
}

/// Canonical parameter type names of JSON execution parameters, matching
/// [`mongreldb_protocol::request::ParameterValue::type_name`].
pub(crate) fn parameter_type_names(params: &[serde_json::Value]) -> Vec<String> {
    params
        .iter()
        .map(|value| {
            match value {
                serde_json::Value::Null => "NULL",
                serde_json::Value::Bool(_) => "BOOL",
                serde_json::Value::Number(number) => {
                    if number.is_i64() || number.is_u64() {
                        "INT64"
                    } else {
                        "FLOAT64"
                    }
                }
                serde_json::Value::String(_) => "TEXT",
                // Arrays/objects are rejected by `render_sql_literal` before
                // binding; map them anyway so the check stays total.
                serde_json::Value::Array(_) | serde_json::Value::Object(_) => "BYTES",
            }
            .to_owned()
        })
        .collect()
}

/// Validate declared parameter type names against the canonical set. Anything
/// else is a client error (400) at prepare time.
pub(crate) fn validate_parameter_type_names(names: &[String]) -> Result<(), String> {
    const CANONICAL: [&str; 6] = ["NULL", "BOOL", "INT64", "FLOAT64", "TEXT", "BYTES"];
    for name in names {
        if !CANONICAL.contains(&name.as_str()) {
            return Err(format!(
                "unknown parameter type {name:?}; expected one of {CANONICAL:?}"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
    use std::sync::Arc;
    use tempfile::tempdir;

    fn int64_pk_schema(id: u16, name: &str) -> Schema {
        Schema {
            columns: vec![ColumnDef {
                id,
                name: name.into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            }],
            ..Schema::default()
        }
    }

    #[test]
    fn catalog_state_changes_on_ddl() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        let empty = CatalogState::capture(&db);
        assert!(empty.schema_versions.is_empty());

        db.create_table("items", int64_pk_schema(1, "id")).unwrap();
        let with_table = CatalogState::capture(&db);
        assert_ne!(empty.catalog_version, with_table.catalog_version);
        assert_eq!(with_table.schema_versions.len(), 1);

        // A second table bumps the catalog version and extends the map.
        db.create_table("more", int64_pk_schema(1, "id")).unwrap();
        let with_two = CatalogState::capture(&db);
        assert_ne!(with_table.catalog_version, with_two.catalog_version);
        assert_eq!(with_two.schema_versions.len(), 2);

        // Drop + recreate the same name: fresh table id, fresh version, and
        // the dropped entry stops counting immediately (a plan must never
        // stay valid against a dropped table).
        let (first_id, first_version) = with_two
            .schema_versions
            .iter()
            .next()
            .map(|(id, version)| (*id, *version))
            .unwrap();
        db.drop_table("items").unwrap();
        let after_drop = CatalogState::capture(&db);
        assert_eq!(after_drop.schema_versions.len(), 1);
        assert!(!after_drop.schema_versions.contains_key(&first_id));
        db.create_table("items", int64_pk_schema(1, "id")).unwrap();
        let recreated = CatalogState::capture(&db);
        assert!(
            !recreated.schema_versions.contains_key(&first_id)
                || recreated.schema_versions.get(&first_id) != Some(&first_version),
            "drop+recreate must not resurrect the old binding entry"
        );
    }

    // ID: P0.4-X13 Stale schema forces prepared replan (binding incompatible).
    #[test]
    fn binding_compatibility_follows_catalog_state() {
        let dir = tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path()).unwrap());
        db.create_table("items", int64_pk_schema(1, "id")).unwrap();
        let planned = CatalogState::capture(&db);
        let binding = build_binding(
            StatementId::new(1),
            "SELECT id FROM items".to_owned(),
            vec![],
            &planned,
        );
        assert!(planned.is_compatible(&binding));

        db.create_table("other", int64_pk_schema(1, "id")).unwrap();
        let changed = CatalogState::capture(&db);
        assert!(
            !changed.is_compatible(&binding),
            "any catalog change invalidates the binding (catalog version moves)"
        );
    }

    #[test]
    fn json_parameters_map_to_canonical_type_names() {
        let params = vec![
            serde_json::Value::Null,
            serde_json::json!(true),
            serde_json::json!(-42),
            serde_json::json!(2.5),
            serde_json::json!("text"),
            serde_json::json!([1, 2]),
        ];
        assert_eq!(
            parameter_type_names(&params),
            vec!["NULL", "BOOL", "INT64", "FLOAT64", "TEXT", "BYTES"]
        );
        assert!(parameter_type_names(&[]).is_empty());
    }

    #[test]
    fn declared_parameter_types_are_validated() {
        assert!(validate_parameter_type_names(&[]).is_ok());
        assert!(validate_parameter_type_names(&["INT64".to_owned(), "TEXT".to_owned()]).is_ok());
        assert!(validate_parameter_type_names(&["int64".to_owned()]).is_err());
        assert!(validate_parameter_type_names(&["DATE".to_owned()]).is_err());
    }
}
