use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{MongrelError, Result};
use crate::memtable::Value;

/// Logical column types. The on-disk Arrow encoding is chosen at flush based on
/// [`TypeId`] and run-time stats (e.g. low-cardinality strings → dictionary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TypeId {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    TimestampNanos,
    Date32,
    /// Variable-length bytes (covers UTF-8 strings).
    Bytes,
    /// Fixed-size binary embedding of `dim` f32 components.
    Embedding {
        dim: u32,
    },
}

impl TypeId {
    /// Fixed size in bytes for fixed-width types, else `None`.
    pub const fn fixed_size(self) -> Option<usize> {
        match self {
            TypeId::Bool => Some(1),
            TypeId::Int8 | TypeId::UInt8 => Some(1),
            TypeId::Int16 | TypeId::UInt16 => Some(2),
            TypeId::Int32 | TypeId::UInt32 | TypeId::Float32 | TypeId::Date32 => Some(4),
            TypeId::Int64 | TypeId::UInt64 | TypeId::Float64 | TypeId::TimestampNanos => Some(8),
            TypeId::Bytes | TypeId::Embedding { .. } => None,
        }
    }
}

/// Per-column flags packed into a `u32`. Stored verbatim in the run header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnFlags {
    bits: u32,
}

impl ColumnFlags {
    pub const NULLABLE: u32 = 1 << 0;
    pub const PRIMARY_KEY: u32 = 1 << 1;
    pub const ENCRYPTED: u32 = 1 << 2;
    /// Store HMAC(value) for equality or OPE for range so indexes work without
    /// decrypting.
    pub const ENCRYPTED_INDEXABLE: u32 = 1 << 3;
    /// Store 1 bit per dimension; similarity via popcount(XOR).
    pub const EMBEDDING_BINARY_QUANTIZED: u32 = 1 << 4;
    /// Engine-managed monotonic identity allocator. Valid only on a single
    /// `Int64` primary-key column per table (see [`Schema::validate_auto_increment`]).
    /// On insert, when the column is omitted or `Null`, the engine assigns the
    /// next counter value; an explicit `Int64` value is honored and advances the
    /// counter past it. Counters are 1-based, never reused, and independent of
    /// the physical [`crate::rowid::RowId`].
    pub const AUTO_INCREMENT: u32 = 1 << 5;

    #[inline]
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    #[inline]
    pub const fn with(mut self, flag: u32) -> Self {
        self.bits |= flag;
        self
    }

    #[inline]
    pub const fn contains(&self, flag: u32) -> bool {
        self.bits & flag != 0
    }

    #[inline]
    pub const fn bits(&self) -> u32 {
        self.bits
    }
}

/// A column definition. `id` is stable, monotonic, and never reused.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u16,
    pub name: String,
    pub ty: TypeId,
    pub flags: ColumnFlags,
}

/// The kind of secondary index to maintain for a column. The primary-key index
/// (in-memory HOT + on-disk learned PGM) is implicit and not listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    /// Roaring bitmap (value → row-id set). Low-cardinality equality / IN.
    Bitmap,
    /// FM-index / wavelet tree for arbitrary substring + ranked access.
    FmIndex,
    /// Quantized-vector ANN (binary / PQ). For `Embedding` columns.
    Ann,
    /// Learned zonemap (PGM) for ordered range predicates.
    LearnedRange,
    /// MinHash/LSH set-similarity (AI dedup/join primitives).
    MinHash,
    /// Learned-sparse (SPLADE-style) retrieval over weighted token vectors.
    Sparse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub column_id: u16,
    pub kind: IndexKind,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    pub schema_id: u64,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    /// Phase 18.2: column co-location groups. Each inner Vec lists column IDs
    /// that are always accessed together. The run writer writes their pages
    /// adjacently so a scan touching those columns benefits from sequential
    /// I/O and cache locality. Empty = no co-location (default).
    #[serde(default)]
    pub colocation: Vec<Vec<u16>>,
}

impl Schema {
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn primary_key(&self) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.flags.contains(ColumnFlags::PRIMARY_KEY))
    }

    /// Return an error if any column that is not marked NULLABLE is either
    /// missing from `columns` or present as `Value::Null`. A column carrying
    /// [`ColumnFlags::AUTO_INCREMENT`] is exempt when omitted/`Null` because the
    /// engine fills it in before this check runs.
    pub fn validate_not_null(&self, columns: &HashMap<u16, Value>) -> Result<()> {
        for col in &self.columns {
            if col.flags.contains(ColumnFlags::NULLABLE) {
                continue;
            }
            // The engine supplies the AUTO_INCREMENT value, so its absence is
            // legal at this layer (filled in upstream of validation).
            if col.flags.contains(ColumnFlags::AUTO_INCREMENT) {
                match columns.get(&col.id) {
                    None | Some(Value::Null) => continue,
                    Some(_) => {}
                }
            }
            match columns.get(&col.id) {
                None => {
                    return Err(MongrelError::InvalidArgument(format!(
                        "column '{}' ({}) is NOT NULL but was omitted",
                        col.name, col.id
                    )));
                }
                Some(Value::Null) => {
                    return Err(MongrelError::InvalidArgument(format!(
                        "column '{}' ({}) is NOT NULL but got NULL",
                        col.name, col.id
                    )));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Enforce the `AUTO_INCREMENT` column contract: at most one such column,
    /// and it must be a non-nullable `Int64` primary key. Called at table
    /// creation time so an invalid schema never reaches the insert path.
    pub fn validate_auto_increment(&self) -> Result<()> {
        let mut seen: Option<&ColumnDef> = None;
        for col in &self.columns {
            if !col.flags.contains(ColumnFlags::AUTO_INCREMENT) {
                continue;
            }
            if seen.is_some() {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT may be set on at most one column; '{}' and '{}' both carry it",
                    seen.unwrap().name,
                    col.name
                )));
            }
            if col.ty != TypeId::Int64 {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT column '{}' must be Int64, is {:?}",
                    col.name, col.ty
                )));
            }
            if !col.flags.contains(ColumnFlags::PRIMARY_KEY) {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT column '{}' must also be the primary key",
                    col.name
                )));
            }
            if col.flags.contains(ColumnFlags::NULLABLE) {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT column '{}' must not be nullable",
                    col.name
                )));
            }
            seen = Some(col);
        }
        Ok(())
    }

    /// The single `AUTO_INCREMENT` column, if any.
    pub fn auto_increment_column(&self) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.flags.contains(ColumnFlags::AUTO_INCREMENT))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_composition() {
        let f = ColumnFlags::empty()
            .with(ColumnFlags::PRIMARY_KEY)
            .with(ColumnFlags::ENCRYPTED_INDEXABLE);
        assert!(f.contains(ColumnFlags::PRIMARY_KEY));
        assert!(f.contains(ColumnFlags::ENCRYPTED_INDEXABLE));
        assert!(!f.contains(ColumnFlags::ENCRYPTED));
    }

    #[test]
    fn fixed_size() {
        assert_eq!(TypeId::Int64.fixed_size(), Some(8));
        assert_eq!(TypeId::Bytes.fixed_size(), None);
        assert_eq!(TypeId::Embedding { dim: 768 }.fixed_size(), None);
    }

    fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
        ColumnDef {
            id,
            name: name.into(),
            ty,
            flags,
        }
    }

    #[test]
    fn auto_increment_validation_accepts_int64_pk() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col(0, "id", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT))],
            indexes: vec![],
            colocation: vec![],
        };
        assert!(s.validate_auto_increment().is_ok());
        assert_eq!(s.auto_increment_column().unwrap().id, 0);
    }

    #[test]
    fn auto_increment_validation_rejects_non_pk() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(0, "id", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY)),
                col(1, "seq", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT)),
            ],
            indexes: vec![],
            colocation: vec![],
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_validation_rejects_non_int64() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col(0, "id", TypeId::Bytes, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT))],
            indexes: vec![],
            colocation: vec![],
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_validation_rejects_two() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(0, "id", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT)),
                col(1, "id2", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT)),
            ],
            indexes: vec![],
            colocation: vec![],
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_exempt_from_not_null_when_omitted() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(0, "id", TypeId::Int64, ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT)),
                col(1, "name", TypeId::Bytes, ColumnFlags::empty()),
            ],
            indexes: vec![],
            colocation: vec![],
        };
        // Omitting the auto-inc column must not trip NOT NULL.
        let mut cols = HashMap::new();
        cols.insert(1u16, Value::Bytes(b"x".to_vec()));
        assert!(s.validate_not_null(&cols).is_ok());
    }
}
