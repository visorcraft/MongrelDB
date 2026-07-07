use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::constraint::TableConstraints;
use crate::error::{MongrelError, Result};
use crate::memtable::Value;

/// Logical column types. The on-disk Arrow encoding is chosen at flush based on
/// [`TypeId`] and run-time stats (e.g. low-cardinality strings → dictionary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Millisecond-precision date (days since epoch × 86400000). Same i64
    /// storage as TimestampNanos; distinct for SQL type affinity.
    Date64,
    /// Nanosecond-precision time-of-day (no date component). Stored as i64.
    Time64,
    /// SQL INTERVAL (months + days + nanoseconds). Stored as 16 bytes
    /// (i64 months, i32 days, i64 nanos).
    Interval,
    /// RFC 4122 UUID. Stored as 16-byte fixed-width (big-endian for sort order).
    Uuid,
    /// JSON value stored as UTF-8 bytes. Distinct from `Bytes` at the type level
    /// so SQL functions and clients know to parse/validate JSON.
    Json,
    /// Variable-length array of homogeneous values (e.g. `int[]`, `text[]`).
    /// Stored as JSON arrays in a Bytes column (SQL-level typed as Array).
    /// The `element_type` is advisory — the Kit layer and DataFusion handle
    /// the actual element encoding.
    Array {
        element_type: u8,
    },
    /// Variable-length bytes (covers UTF-8 strings).
    Bytes,
    /// Fixed-size binary embedding of `dim` f32 components.
    Embedding {
        dim: u32,
    },
    /// Fixed-point decimal (i128 unscaled value, precision, scale). SQL:
    /// `mongreldb_decimal(precision, scale)` or `DECIMAL(p, s)`.
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    /// SQL ENUM: stored as `Value::Bytes(variant_name_utf8)`, validated against
    /// the `variants` list at write time. Dictionary-encoded on disk like
    /// `Bytes` (low-cardinality sweet spot). Membership is enforced at the
    /// write edge (SQL `coerce_value`, HTTP `json_to_value`), not at the core
    /// commit path.
    Enum {
        variants: Arc<[String]>,
    },
}

impl TypeId {
    /// Fixed size in bytes for fixed-width types, else `None`.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            TypeId::Bool => Some(1),
            TypeId::Int8 | TypeId::UInt8 => Some(1),
            TypeId::Int16 | TypeId::UInt16 => Some(2),
            TypeId::Int32 | TypeId::UInt32 | TypeId::Float32 | TypeId::Date32 => Some(4),
            TypeId::Int64
            | TypeId::UInt64
            | TypeId::Float64
            | TypeId::TimestampNanos
            | TypeId::Date64
            | TypeId::Time64 => Some(8),
            TypeId::Bytes | TypeId::Embedding { .. } | TypeId::Enum { .. } => None,
            TypeId::Decimal128 { .. } => Some(16),
            TypeId::Uuid => Some(16),
            TypeId::Json | TypeId::Array { .. } => None,
            TypeId::Interval => Some(20), // i64 months + i32 days + i64 nanos
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
    pub const fn without(mut self, flag: u32) -> Self {
        self.bits &= !flag;
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

/// A default-value expression stored on a column definition and applied
/// authoritatively by the engine at insert stage time (before NOT NULL
/// validation) when the column is omitted or explicitly `Null`. Sequence
/// defaults are handled separately via [`ColumnFlags::AUTO_INCREMENT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DefaultExpr {
    /// A literal value applied verbatim.
    Static(Value),
    /// Current timestamp as an ISO-8601 UTC string (`Value::Bytes`). Resolved
    /// at stage time (per-row).
    Now,
    /// A random RFC 4122 UUID (`Value::Uuid`). Resolved at stage time.
    Uuid,
}

/// A column definition. `id` is stable, monotonic, and never reused.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u16,
    pub name: String,
    pub ty: TypeId,
    pub flags: ColumnFlags,
    /// Optional default expression applied at insert stage time when the column
    /// is omitted or explicitly `Null`. Serialized for catalog persistence;
    /// old catalogs without this field deserialize to `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<DefaultExpr>,
}

/// Metadata updates supported by native ALTER COLUMN.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlterColumn {
    pub name: Option<String>,
    pub ty: Option<TypeId>,
    pub flags: Option<ColumnFlags>,
    /// `None` = leave default unchanged, `Some(None)` = drop default,
    /// `Some(Some(expr))` = set/replace default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Option<DefaultExpr>>,
}

impl AlterColumn {
    pub fn rename(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ty: None,
            flags: None,
            default_value: None,
        }
    }

    pub fn set_type(ty: TypeId) -> Self {
        Self {
            name: None,
            ty: Some(ty),
            flags: None,
            default_value: None,
        }
    }

    pub fn set_flags(flags: ColumnFlags) -> Self {
        Self {
            name: None,
            ty: None,
            flags: Some(flags),
            default_value: None,
        }
    }

    pub fn set_default(expr: DefaultExpr) -> Self {
        Self {
            name: None,
            ty: None,
            flags: None,
            default_value: Some(Some(expr)),
        }
    }

    pub fn drop_default() -> Self {
        Self {
            name: None,
            ty: None,
            flags: None,
            default_value: Some(None),
        }
    }
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
    /// Partial index predicate: a SQL WHERE clause expression serialized as
    /// a string (e.g. `"deleted_at IS NULL"`). Only rows matching this
    /// predicate are indexed. `None` means all rows are indexed (full index).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
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
    /// Engine-side declarative constraints (unique / FK / check). Empty by
    /// default — legacy and Kit-managed tables carry no engine constraints and
    /// behave exactly as before. When non-empty, the transaction layer enforces
    /// them authoritatively at commit (see [`crate::database`]).
    #[serde(default)]
    pub constraints: TableConstraints,
    /// When true, the table is clustered on its primary key: sorted runs are
    /// keyed by PK bytes rather than by `RowId`. Defaults to false.
    #[serde(default)]
    pub clustered: bool,
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
    pub fn validate_not_null(&self, columns: &[(u16, Value)]) -> Result<()> {
        // Rows are short sparse `(id, value)` lists; a linear probe beats
        // materializing a HashMap (and cloning every Value) per row.
        let at = |id: u16| columns.iter().find(|(c, _)| *c == id).map(|(_, v)| v);
        for col in &self.columns {
            if col.flags.contains(ColumnFlags::NULLABLE) {
                continue;
            }
            // The engine supplies the AUTO_INCREMENT value, so its absence is
            // legal at this layer (filled in upstream of validation).
            if col.flags.contains(ColumnFlags::AUTO_INCREMENT) {
                match at(col.id) {
                    None | Some(Value::Null) => continue,
                    Some(_) => {}
                }
            }
            match at(col.id) {
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
            if let Some(prev) = seen {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT may be set on at most one column; '{}' and '{}' both carry it",
                    prev.name, col.name
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

    /// Validate that every column carrying a `default_value` has a
    /// type-compatible expression. Called at table creation and ALTER COLUMN
    /// so an invalid default never reaches the insert path.
    pub fn validate_defaults(&self) -> Result<()> {
        for col in &self.columns {
            let Some(expr) = &col.default_value else {
                continue;
            };
            match expr {
                DefaultExpr::Static(v) => {
                    if !value_matches_type(v, col.ty.clone()) {
                        return Err(MongrelError::Schema(format!(
                            "DEFAULT value for column '{}' ({:?}) does not match type {:?}",
                            col.name, v, col.ty
                        )));
                    }
                }
                DefaultExpr::Now => {
                    if !matches!(
                        col.ty,
                        TypeId::Bytes | TypeId::TimestampNanos | TypeId::Date64
                    ) {
                        return Err(MongrelError::Schema(format!(
                            "DEFAULT NOW() on column '{}' requires Bytes/TimestampNanos/Date64, is {:?}",
                            col.name, col.ty
                        )));
                    }
                }
                DefaultExpr::Uuid => {
                    if !matches!(col.ty, TypeId::Uuid | TypeId::Bytes) {
                        return Err(MongrelError::Schema(format!(
                            "DEFAULT UUID() on column '{}' requires Uuid/Bytes, is {:?}",
                            col.name, col.ty
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Check that a [`Value`] is compatible with a [`TypeId`] for default-value
/// validation. More lenient than full type-checking: `Null` is universally
/// accepted (it means "DEFAULT NULL"), and `Bytes` covers UTF-8 string types.
pub(crate) fn value_matches_type(v: &Value, ty: TypeId) -> bool {
    matches!(
        (v, ty),
        (Value::Null, _)
            | (Value::Bool(_), TypeId::Bool)
            | (
                Value::Int64(_),
                TypeId::Int8 | TypeId::Int16 | TypeId::Int32 | TypeId::Int64
            )
            | (Value::Float64(_), TypeId::Float32 | TypeId::Float64)
            | (
                Value::Bytes(_),
                TypeId::Bytes
                    | TypeId::Json
                    | TypeId::Uuid
                    | TypeId::Date64
                    | TypeId::Time64
                    | TypeId::Enum { .. }
            )
            | (
                Value::Int64(_),
                TypeId::TimestampNanos | TypeId::Date32 | TypeId::Date64 | TypeId::Time64
            )
            | (Value::Uuid(_), TypeId::Uuid)
            | (Value::Decimal(_), TypeId::Decimal128 { .. })
            | (Value::Json(_), TypeId::Json)
            | (Value::Embedding(_), TypeId::Embedding { .. })
            | (Value::Interval { .. }, TypeId::Interval)
    )
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
            default_value: None,
        }
    }

    #[test]
    fn auto_increment_validation_accepts_int64_pk() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_ok());
        assert_eq!(s.auto_increment_column().unwrap().id, 0);
    }

    #[test]
    fn auto_increment_validation_rejects_non_pk() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(
                    0,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                ),
                col(
                    1,
                    "seq",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_validation_rejects_non_int64() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col(
                0,
                "id",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_validation_rejects_two() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(
                    0,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty()
                        .with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
                ),
                col(
                    1,
                    "id2",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_exempt_from_not_null_when_omitted() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(
                    0,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty()
                        .with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
                ),
                col(1, "name", TypeId::Bytes, ColumnFlags::empty()),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        // Omitting the auto-inc column must not trip NOT NULL.
        let cols = vec![(1u16, Value::Bytes(b"x".to_vec()))];
        assert!(s.validate_not_null(&cols).is_ok());
    }

    fn col_with_default(
        id: u16,
        name: &str,
        ty: TypeId,
        flags: ColumnFlags,
        dv: DefaultExpr,
    ) -> ColumnDef {
        ColumnDef {
            id,
            name: name.into(),
            ty,
            flags,
            default_value: Some(dv),
        }
    }

    #[test]
    fn validate_defaults_accepts_matching_static() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "active",
                TypeId::Bool,
                ColumnFlags::empty(),
                DefaultExpr::Static(Value::Bool(true)),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_defaults().is_ok());
    }

    #[test]
    fn validate_defaults_rejects_mismatched_static() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "count",
                TypeId::Int64,
                ColumnFlags::empty(),
                DefaultExpr::Static(Value::Bytes(b"oops".to_vec())),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_defaults().is_err());
    }

    #[test]
    fn validate_defaults_now_requires_temporal_or_bytes() {
        let ok = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "ts",
                TypeId::Bytes,
                ColumnFlags::empty(),
                DefaultExpr::Now,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(ok.validate_defaults().is_ok());

        let bad = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "ts",
                TypeId::Int64,
                ColumnFlags::empty(),
                DefaultExpr::Now,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(bad.validate_defaults().is_err());
    }

    #[test]
    fn validate_defaults_uuid_requires_uuid_or_bytes() {
        let ok = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "id",
                TypeId::Uuid,
                ColumnFlags::empty(),
                DefaultExpr::Uuid,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(ok.validate_defaults().is_ok());

        let bad = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "id",
                TypeId::Bool,
                ColumnFlags::empty(),
                DefaultExpr::Uuid,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(bad.validate_defaults().is_err());
    }

    #[test]
    fn serde_roundtrip_column_def_with_default() {
        let c = col_with_default(
            0,
            "x",
            TypeId::Bytes,
            ColumnFlags::empty(),
            DefaultExpr::Static(Value::Bytes(b"hello".to_vec())),
        );
        let json = serde_json::to_string(&c).unwrap();
        let de: ColumnDef = serde_json::from_str(&json).unwrap();
        assert_eq!(c, de);
        // ColumnDef without default deserializes to None.
        let old_json = r#"{"id":0,"name":"y","ty":{"kind":"bytes"},"flags":{"bits":0}}"#;
        let old: ColumnDef = serde_json::from_str(old_json).unwrap();
        assert!(old.default_value.is_none());
    }
}
