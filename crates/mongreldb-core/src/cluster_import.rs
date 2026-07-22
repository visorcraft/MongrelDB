//! Standalone → cluster import groundwork (spec section 5.2, Stage 2E).
//!
//! The first cluster release never converts a database in place. The import
//! process is:
//!
//! 1. Open the source read-only.  ← [`cluster_import_prepare`] (this module)
//! 2. Capture a consistent snapshot. ← this module
//! 3. Create a new cluster database and initial tablet. — server/cluster wave
//! 4. Stream rows and schema into the replicated tablet. — server/cluster wave
//! 5. Validate counts, hashes, constraints, and index definitions. ← the
//!    [`ImportPlan`] carries every count and hash that step needs
//! 6. Publish the new database. — server/cluster wave
//! 7. Leave the source untouched. ← the read-only offline open guarantees it
//!
//! This module is the library form of steps 1–2 and the validation inputs of
//! step 5: it opens the source through the read-only offline-validation API
//! (spec section 5.3), pins one consistent MVCC epoch across every table,
//! and produces the deterministic stream plan — per-table schema (including
//! index and constraint definitions), row counts, and content hashes. The
//! streamed write into a replicated tablet lands with the server/cluster
//! wave; this plan is what that wave validates against.

use std::path::Path;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::memtable::Row;
use crate::schema::Schema;
use crate::storage_mode::StorageMode;
use crate::{MongrelError, Result};

/// One table's slice of an [`ImportPlan`]: the complete schema (columns,
/// index definitions, constraints — everything the replicated tablet must
/// recreate) plus the deterministic row-stream validation totals.
#[derive(Debug, Clone)]
pub struct ImportTablePlan {
    /// Catalog table id in the source.
    pub table_id: u64,
    /// Catalog name.
    pub name: String,
    /// Full source schema (columns, indexes, constraints).
    pub schema: Schema,
    /// Rows visible at the plan's snapshot epoch.
    pub row_count: u64,
    /// SHA-256 over the table's canonical row stream (see
    /// [`hash_rows_canonical`]).
    pub rows_sha256: [u8; 32],
}

/// A deterministic, validation-ready plan for streaming one standalone
/// database into a fresh cluster database (spec section 5.2).
#[derive(Debug, Clone)]
pub struct ImportPlan {
    /// Name of the cluster database the import targets.
    pub database: String,
    /// The source's storage mode (informational; the import source is
    /// normally [`StorageMode::Standalone`], `None` for pre-marker databases).
    pub source_storage_mode: Option<StorageMode>,
    /// The consistent MVCC epoch every table was read at.
    pub snapshot_epoch: u64,
    /// Per-table plans, ordered by table id (deterministic stream order).
    pub tables: Vec<ImportTablePlan>,
    /// Total rows across every table.
    pub total_rows: u64,
    /// SHA-256 over every table's canonical schema encoding, in stream order.
    pub schema_sha256: [u8; 32],
    /// SHA-256 over every table's `rows_sha256`, in stream order.
    pub rows_sha256: [u8; 32],
}

/// Canonical, order-independent SHA-256 over a row set.
///
/// Rows are sorted by [`crate::RowId`]; each row contributes its id, commit
/// epoch, delete flag, and its columns sorted by column id (the in-memory
/// `HashMap` column order is process-random and must never leak into a
/// validation hash). Two databases holding the same logical rows at the same
/// epochs hash identically in any process.
pub fn hash_rows_canonical(rows: &[Row]) -> [u8; 32] {
    let mut ordered: Vec<&Row> = rows.iter().collect();
    ordered.sort_by_key(|row| row.row_id);
    let mut hasher = Sha256::new();
    for row in ordered {
        hasher.update(row.row_id.0.to_le_bytes());
        hasher.update(row.committed_epoch.0.to_le_bytes());
        hasher.update([u8::from(row.deleted)]);
        let mut columns: Vec<(&u16, &crate::memtable::Value)> = row.columns.iter().collect();
        columns.sort_by_key(|(column_id, _)| *column_id);
        hasher.update((columns.len() as u64).to_le_bytes());
        for (column_id, value) in columns {
            hasher.update(column_id.to_le_bytes());
            let encoded =
                bincode::serialize(value).expect("Value serialization is infallible for hashing");
            hasher.update((encoded.len() as u64).to_le_bytes());
            hasher.update(encoded);
        }
    }
    hasher.finalize().into()
}

/// Canonical JSON encoding of `value` with object keys sorted recursively,
/// so structurally equal values hash identically regardless of any map
/// ordering inside them.
fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut value = serde_json::to_value(value)
        .map_err(|error| MongrelError::Other(format!("canonical json: {error}")))?;
    canonicalize(&mut value);
    serde_json::to_vec(&value)
        .map_err(|error| MongrelError::Other(format!("canonical json: {error}")))
}

fn canonicalize(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            map.sort_keys();
            for value in map.values_mut() {
                canonicalize(value);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                canonicalize(item);
            }
        }
        _ => {}
    }
}

/// Steps 1–2 of the spec section 5.2 import process: open `source` read-only
/// (the offline-validation API of spec section 5.3 — any storage mode opens,
/// and no write can reach the source), pin one consistent MVCC epoch, and
/// produce the row/schema stream plan with the counts and hashes step 5
/// validates. `database` is the name of the cluster database the plan
/// targets; it is recorded, not created (creation lands with the
/// server/cluster wave).
pub fn cluster_import_prepare(source: impl AsRef<Path>, database: &str) -> Result<ImportPlan> {
    if database.is_empty() {
        return Err(MongrelError::InvalidArgument(
            "cluster import requires a target database name".into(),
        ));
    }
    let options = crate::OpenOptions::default().with_offline_validation(true);
    let db = crate::Database::open_with_options(source.as_ref(), options)?;
    let source_storage_mode = db.storage_mode()?;
    let snapshot_epoch = db.visible_epoch();
    // P0.5: HLC-stamped versions require an HLC-pinned snapshot.
    let snapshot = db.snapshot_for_epoch(snapshot_epoch);

    let mut tables = Vec::new();
    for name in db.table_names() {
        let table_id = db.table_id(&name)?;
        let handle = db.table(&name)?;
        let (schema, rows) = {
            let table = handle.lock();
            (table.schema().clone(), table.visible_rows(snapshot)?)
        };
        tables.push(ImportTablePlan {
            table_id,
            name,
            schema,
            row_count: rows.len() as u64,
            rows_sha256: hash_rows_canonical(&rows),
        });
    }
    drop(db);
    tables.sort_by_key(|table| table.table_id);

    let mut schema_hasher = Sha256::new();
    let mut rows_hasher = Sha256::new();
    let mut total_rows = 0_u64;
    for table in &tables {
        let encoded = canonical_json(&table.schema)?;
        schema_hasher.update((encoded.len() as u64).to_le_bytes());
        schema_hasher.update(encoded);
        rows_hasher.update(table.rows_sha256);
        total_rows = total_rows.saturating_add(table.row_count);
    }
    Ok(ImportPlan {
        database: database.to_string(),
        source_storage_mode,
        snapshot_epoch: snapshot_epoch.0,
        tables,
        total_rows,
        schema_sha256: schema_hasher.finalize().into(),
        rows_sha256: rows_hasher.finalize().into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;
    use crate::rowid::RowId;

    #[test]
    fn canonical_row_hash_is_order_independent() {
        let mut a = Row::new(RowId(1), crate::epoch::Epoch(3));
        a.columns.insert(1, Value::Int64(10));
        a.columns.insert(2, Value::Bytes(b"x".to_vec()));
        a.columns.insert(7, Value::Null);
        let mut b = Row::new(RowId(2), crate::epoch::Epoch(3));
        b.columns.insert(1, Value::Bool(true));

        let forward = hash_rows_canonical(&[a.clone(), b.clone()]);
        let backward = hash_rows_canonical(&[b, a.clone()]);
        assert_eq!(forward, backward);

        let mut changed = Row::new(RowId(2), crate::epoch::Epoch(3));
        changed.columns.insert(1, Value::Bool(false));
        assert_ne!(forward, hash_rows_canonical(&[a, changed]));
    }

    #[test]
    fn canonical_json_sorts_object_keys() {
        let first = canonical_json(&serde_json::json!({"b": 1, "a": {"d": 2, "c": 3}})).unwrap();
        let second = canonical_json(&serde_json::json!({"a": {"c": 3, "d": 2}, "b": 1})).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn import_plan_counts_and_hashes_match_source() {
        use crate::memtable::Value;
        use crate::schema::{ColumnDef, ColumnFlags, Schema, TypeId};

        let dir = tempfile::tempdir().unwrap();
        let db = crate::Database::create(dir.path()).unwrap();
        let schema = Schema {
            columns: vec![ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            }],
            ..Schema::default()
        };
        db.create_table("items", schema).unwrap();
        let mut txn = db.begin();
        for value in [10_i64, 20, 30] {
            txn.put("items", vec![(1, Value::Int64(value))]).unwrap();
        }
        txn.commit().unwrap();
        let snapshot_epoch = db.visible_epoch();
        let handle = db.table("items").unwrap();
        let expected_rows = handle
            .lock()
            .visible_rows(db.snapshot_for_epoch(snapshot_epoch))
            .unwrap();
        drop(handle);
        drop(db);

        let plan = cluster_import_prepare(dir.path(), "app").unwrap();
        assert_eq!(plan.database, "app");
        assert_eq!(plan.source_storage_mode, Some(StorageMode::Standalone));
        assert_eq!(plan.snapshot_epoch, snapshot_epoch.0);
        assert_eq!(plan.tables.len(), 1);
        let table = &plan.tables[0];
        assert_eq!(table.name, "items");
        assert_eq!(table.row_count, 3);
        assert_eq!(table.rows_sha256, hash_rows_canonical(&expected_rows));
        assert_eq!(plan.total_rows, 3);
        assert_eq!(plan.schema_sha256.len(), 32);

        // The plan is reproducible: a second prepare over the untouched
        // source yields identical counts and hashes.
        let second = cluster_import_prepare(dir.path(), "app").unwrap();
        assert_eq!(plan.schema_sha256, second.schema_sha256);
        assert_eq!(plan.rows_sha256, second.rows_sha256);
        assert_eq!(plan.total_rows, second.total_rows);

        // The source is untouched: still opens normally with its rows.
        let db = crate::Database::open(dir.path()).unwrap();
        let handle = db.table("items").unwrap();
        let rows = handle
            .lock()
            .visible_rows(db.snapshot_for_epoch(snapshot_epoch))
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(plan.database, "app");
    }

    #[test]
    fn import_plan_rejects_empty_database_name() {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Database::create(dir.path()).unwrap();
        drop(db);
        assert!(cluster_import_prepare(dir.path(), "").is_err());
    }
}
