//! Secondary-index **delta** maintenance for row replace / update.
//!
//! When a row is rewritten (product `update_many` normalizes to delete+put with
//! a new `RowId`, or a same-PK put replaces an older version), only **Bitmap**
//! secondary keys that actually change need remove+insert work. Unchanged
//! equality keys only re-point membership from the old row id to the new one
//! (or no-op when the row id is unchanged).
//!
//! Pure planning lives here so tests can lock the delta policy without driving
//! the full write path. Application against live maps is
//! [`crate::engine::Table`]'s responsibility via
//! [`apply_bitmap_secondary_delta`].

use crate::index::BitmapIndex;
use crate::memtable::{Row, Value};
use crate::rowid::RowId;
use crate::schema::{IndexKind, Schema};
use std::collections::HashMap;

/// One planned change against a single Bitmap secondary index column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitmapIndexDelta {
    /// Indexed key unchanged: re-point `old_row_id` → `new_row_id` under `key`
    /// when the ids differ; pure no-op when they are equal.
    Repoint {
        column_id: u16,
        key: Vec<u8>,
    },
    /// Indexed key changed (or appeared/disappeared): drop old membership and
    /// insert new when present.
    Move {
        column_id: u16,
        old_key: Option<Vec<u8>>,
        new_key: Option<Vec<u8>>,
    },
}

/// Encode the Bitmap key for a column value if the column is present on the row.
///
/// Matches `index_into`: missing columns are not indexed; present `Null` uses
/// `Value::encode_key()` (empty bytes).
pub fn bitmap_key_for_column(row: &Row, column_id: u16) -> Option<Vec<u8>> {
    row.columns.get(&column_id).map(Value::encode_key)
}

/// Plan Bitmap secondary-index deltas between two row images.
///
/// Only `IndexKind::Bitmap` entries in `schema.indexes` are planned. ANN / FM /
/// Sparse / MinHash / LearnedRange keep their existing full reindex paths for
/// now (different remove semantics).
pub fn plan_bitmap_secondary_deltas(schema: &Schema, old: &Row, new: &Row) -> Vec<BitmapIndexDelta> {
    let mut out = Vec::new();
    for idef in &schema.indexes {
        if idef.kind != IndexKind::Bitmap {
            continue;
        }
        let old_key = bitmap_key_for_column(old, idef.column_id);
        let new_key = bitmap_key_for_column(new, idef.column_id);
        match (old_key, new_key) {
            (Some(o), Some(n)) if o == n => {
                out.push(BitmapIndexDelta::Repoint {
                    column_id: idef.column_id,
                    key: o,
                });
            }
            (old_key, new_key) => {
                out.push(BitmapIndexDelta::Move {
                    column_id: idef.column_id,
                    old_key,
                    new_key,
                });
            }
        }
    }
    out
}

/// Apply planned Bitmap deltas to live index maps.
///
/// `old_row_id` / `new_row_id` are taken from the row images so callers can
/// pass pre-tombstone and post-put identities.
pub fn apply_bitmap_secondary_delta(
    bitmap: &mut HashMap<u16, BitmapIndex>,
    deltas: &[BitmapIndexDelta],
    old_row_id: RowId,
    new_row_id: RowId,
) {
    for delta in deltas {
        match delta {
            BitmapIndexDelta::Repoint { column_id, key } => {
                let Some(b) = bitmap.get_mut(column_id) else {
                    continue;
                };
                if old_row_id != new_row_id {
                    b.remove(key, old_row_id);
                    b.insert(key.clone(), new_row_id);
                }
                // same row id + same key: no secondary work
            }
            BitmapIndexDelta::Move {
                column_id,
                old_key,
                new_key,
            } => {
                let Some(b) = bitmap.get_mut(column_id) else {
                    continue;
                };
                if let Some(k) = old_key {
                    b.remove(k, old_row_id);
                }
                if let Some(k) = new_key {
                    b.insert(k.clone(), new_row_id);
                }
            }
        }
    }
}

/// Convenience: plan + apply Bitmap secondary maintenance for a replace.
pub fn maintain_bitmap_secondary_on_replace(
    schema: &Schema,
    bitmap: &mut HashMap<u16, BitmapIndex>,
    old: &Row,
    new: &Row,
) {
    let deltas = plan_bitmap_secondary_deltas(schema, old, new);
    apply_bitmap_secondary_delta(bitmap, &deltas, old.row_id, new.row_id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{ColumnDef, ColumnFlags, IndexDef, TypeId};

    fn schema_trip_bitmap() -> Schema {
        Schema {
            schema_id: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 2,
                    name: "trip_id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 3,
                    name: "title".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "segments_trip_idx".into(),
                column_id: 2,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: Default::default(),
            }],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    fn row(rid: u64, id: i64, trip: i64, title: &[u8]) -> Row {
        let mut r = Row::new(RowId(rid), crate::epoch::Epoch(1));
        r.columns.insert(1, Value::Int64(id));
        r.columns.insert(2, Value::Int64(trip));
        r.columns.insert(3, Value::Bytes(title.to_vec()));
        r
    }

    #[test]
    fn plan_title_only_change_is_repoint_not_move() {
        let schema = schema_trip_bitmap();
        let old = row(0, 10, 42, b"before");
        let new = row(1, 10, 42, b"after");
        let deltas = plan_bitmap_secondary_deltas(&schema, &old, &new);
        assert_eq!(
            deltas,
            vec![BitmapIndexDelta::Repoint {
                column_id: 2,
                key: Value::Int64(42).encode_key(),
            }]
        );
    }

    #[test]
    fn plan_trip_id_change_is_move() {
        let schema = schema_trip_bitmap();
        let old = row(0, 10, 42, b"x");
        let new = row(1, 10, 99, b"x");
        let deltas = plan_bitmap_secondary_deltas(&schema, &old, &new);
        assert_eq!(
            deltas,
            vec![BitmapIndexDelta::Move {
                column_id: 2,
                old_key: Some(Value::Int64(42).encode_key()),
                new_key: Some(Value::Int64(99).encode_key()),
            }]
        );
    }

    #[test]
    fn apply_repoint_removes_old_rid_under_same_key() {
        let schema = schema_trip_bitmap();
        let mut bitmap = HashMap::new();
        bitmap.insert(2, BitmapIndex::new());
        let old = row(0, 10, 42, b"before");
        let new = row(1, 10, 42, b"after");
        bitmap
            .get_mut(&2)
            .unwrap()
            .insert(Value::Int64(42).encode_key(), old.row_id);
        maintain_bitmap_secondary_on_replace(&schema, &mut bitmap, &old, &new);
        let b = bitmap.get(&2).unwrap();
        assert!(!b.contains(&Value::Int64(42).encode_key(), old.row_id));
        assert!(b.contains(&Value::Int64(42).encode_key(), new.row_id));
    }

    #[test]
    fn apply_move_switches_keys() {
        let schema = schema_trip_bitmap();
        let mut bitmap = HashMap::new();
        bitmap.insert(2, BitmapIndex::new());
        let old = row(0, 10, 42, b"x");
        let new = row(1, 10, 99, b"x");
        bitmap
            .get_mut(&2)
            .unwrap()
            .insert(Value::Int64(42).encode_key(), old.row_id);
        maintain_bitmap_secondary_on_replace(&schema, &mut bitmap, &old, &new);
        let b = bitmap.get(&2).unwrap();
        assert!(!b.contains(&Value::Int64(42).encode_key(), old.row_id));
        assert!(b.contains(&Value::Int64(99).encode_key(), new.row_id));
    }
}
